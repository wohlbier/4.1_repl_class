#include <string>
#include <tuple>
#include <vector>

#include <cilk.h>
#include <memoryweb.h>
extern "C" {
#include <emu_c_utils/layout.h>
}

typedef long Index_t;
typedef long Scalar_t;

/*
 * Overrides default new to always allocate replicated storage for instances
 * of this class. repl_new is intended to be used as a parent class for
 * distributed data structure types.
 */
class repl_new
{
public:
    // Overrides default new to always allocate replicated storage for
    // instances of this class
    static void *
    operator new(std::size_t sz)
    {
        return mw_mallocrepl(sz);
    }
    
    // Overrides default delete to safely free replicated storage
    static void
    operator delete(void * ptr)
    {
        mw_free(ptr);
    }
};

typedef std::vector<std::tuple<Index_t, Scalar_t>> Row_t;
typedef Row_t * pRow_t;
typedef pRow_t * ppRow_t;

class Matrix_t : public repl_new
{
public:
    static Matrix_t * create(Index_t nrows)
    {
        return new Matrix_t(nrows);
    }
    
    Matrix_t() = delete;
    Matrix_t(const Matrix_t &) = delete;
    Matrix_t & operator=(const Matrix_t &) = delete;
    Matrix_t(Matrix_t &&) = delete;
    Matrix_t & operator=(Matrix_t &&) = delete;
    
    // fake build function to watch migrations when adding rows
    // using replicated classes
    void build(Index_t row_idx)
    {
        Row_t tmpRow;
        if (row_idx % 2 == 0)
        {
            tmpRow.push_back(std::make_tuple(0,1));
            tmpRow.push_back(std::make_tuple(3,1));
            tmpRow.push_back(std::make_tuple(5,1));
            tmpRow.push_back(std::make_tuple(7,1));
            tmpRow.push_back(std::make_tuple(12,1));
            tmpRow.push_back(std::make_tuple(14,1));
            tmpRow.push_back(std::make_tuple(27,1));
            tmpRow.push_back(std::make_tuple(31,1));
        }
        else
        {
            tmpRow.push_back(std::make_tuple(1,1));
            tmpRow.push_back(std::make_tuple(7,1));
            tmpRow.push_back(std::make_tuple(10,1));
            tmpRow.push_back(std::make_tuple(14,1));
            tmpRow.push_back(std::make_tuple(18,1));
            tmpRow.push_back(std::make_tuple(27,1));
            tmpRow.push_back(std::make_tuple(28,1));
        }
        
        // bc of replication this does not cause migration
        pRow_t rowPtr = rows_[row_idx];
        
        for (Row_t::iterator it = tmpRow.begin(); it < tmpRow.end(); ++it)
        {
            rowPtr->push_back(*it);
        }
    }

    Index_t * nodelet_addr(Index_t i)
    {
        // dereferencing causes migrations
        return (Index_t *)(rows_ + i);
    }
    
private:
    Matrix_t(Index_t nrows) : nrows_(nrows)
    {
        nrows_per_nodelet_ = nrows_ + nrows_ % NODELETS();

        rows_ = (ppRow_t)mw_malloc1dlong(nrows_);

        // replicate the class across nodelets
        for (Index_t i = 1; i < NODELETS(); ++i)
        {
            memcpy(mw_get_nth(this, i), mw_get_nth(this, 0), sizeof(*this));
        }

        // local mallocs on each nodelet
        for (Index_t i = 0; i < nrows_; ++i)
        {
            cilk_migrate_hint(rows_ + i);
            cilk_spawn allocateRow(i);
        }
        cilk_sync;
    }

    // localalloc a single row
    void allocateRow(Index_t i)
    {
        rows_[i] = new Row_t(); // allocRow must be spawned on correct nlet
    }

    Index_t nrows_;
    Index_t nrows_per_nodelet_;
    ppRow_t rows_;
};

int main(int argc, char* argv[])
{
    starttiming();

    Index_t nrows = 16;

    // 2 migrations from:     0 => 1...7  (round robin)
    // 4 migrations from: 1...7 => 0
    // - 2 from main thread returning from each nodelet
    // - 2 from each spawned thread returning from each nodelet
    Matrix_t * A = Matrix_t::create(nrows);

    // double migrations from above
    // 4 migrations from:     0 => 1...7
    // 8 migrations from: 1...7 => 0
    Matrix_t * B = Matrix_t::create(nrows);

    Index_t row_idx_1 = 2; // row on nodelet 2
    cilk_migrate_hint(A->nodelet_addr(row_idx_1));
    // adds one migration  0 => 2
    // adds two migrations 2 => 0, main thread and spawned thread
    cilk_spawn A->build(row_idx_1);
    /*
      MEMORY MAP
      2359,4,5,4,4,4,4,4
      8,234,0,0,0,0,0,0
      10,0,1556,0,0,0,0,0
      8,0,0,234,0,0,0,0
      8,0,0,0,234,0,0,0
      8,0,0,0,0,234,0,0
      8,0,0,0,0,0,234,0
      8,0,0,0,0,0,0,234
    */

    Index_t row_idx_2 = 13; // row on nodelet 5
    cilk_migrate_hint(B->nodelet_addr(row_idx_2));
    // adds one migration 2 => 5    // NEEDS EXPLANATION
    // adds one migration 5 => 2    // NEEDS EXPLANATION
    // adds one migration 5 => 0
    cilk_spawn B->build(row_idx_2);
    /*
      2332,4,5,4,4,4,4,4
      8,234,0,0,0,0,0,0
      10,0,1607,0,0,1,0,0
      8,0,0,234,0,0,0,0
      8,0,0,0,234,0,0,0
      9,0,1,0,0,1513,0,0
      8,0,0,0,0,0,234,0
      8,0,0,0,0,0,0,234
    */

    cilk_sync;
    
    return 0;
}
