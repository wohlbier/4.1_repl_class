#include <string>
#include <tuple>
#include <vector>

#include <cilk.h>
#include <memoryweb.h>
#include <distributed.h>
extern "C" {
#include <emu_c_utils/layout.h>
#include <emu_c_utils/hooks.h>
}

typedef long Index_t;
typedef long Scalar_t;
typedef std::vector<std::tuple<Index_t, Scalar_t>> Row_t;
typedef Row_t * pRow_t;
typedef pRow_t * ppRow_t;

static inline Index_t n_map(Index_t i) { return i % NODELETS(); }
static inline Index_t r_map(Index_t i) { return i / NODELETS(); }

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
        pRow_t rowPtr = rows_[n_map(row_idx)] + r_map(row_idx);
        
        for (Row_t::iterator it = tmpRow.begin(); it < tmpRow.end(); ++it)
        {
            rowPtr->push_back(*it);
        }
    }

    Index_t * nodelet_addr(Index_t i)
    {
      // dereferencing causes migrations
      return (Index_t *)(rows_ + n_map(i));
    }
    
private:
    Matrix_t(Index_t nrows) : nrows_(nrows)
    {
        nrows_per_nodelet_ = r_map(nrows_) + n_map(nrows_); 
        rows_ = (ppRow_t)mw_malloc2d(NODELETS(),
                                     nrows_per_nodelet_ * sizeof(Row_t));

        // replicate the class across nodelets
        for (Index_t i = 1; i < NODELETS(); ++i)
        {
            memcpy(mw_get_nth(this, i), mw_get_nth(this, 0), sizeof(*this));
        }

        // local mallocs on each nodelet
        for (Index_t i = 0; i < NODELETS(); ++i)
        {
            cilk_migrate_hint(rows_ + i);
            cilk_spawn allocateRows(i);
        }
        cilk_sync;
    }

    // localalloc a single row
    void allocateRows(Index_t i)
    {
        for (Index_t row_idx= 0; row_idx < nrows_per_nodelet_; ++row_idx)
        {
            new(rows_[i] + row_idx) Row_t();
        }
    }

    Index_t nrows_;
    Index_t nrows_per_nodelet_;
    ppRow_t rows_;
};

int main(int argc, char* argv[])
{
    Index_t nrows = 16;
    hooks_region_begin("GBTL_Matrix_Build");

    /*
    Nodelets start at 0 and end at 7
    Matrix A will have 2 rows per nodelet, total 2RowsX8Nodelets
    
    Expected Migration:
    	Thread 0 migrates to each nodelet, spawns 1 thread and returns back to nodelet 0.
    	Spawned thread does the allocation for all rows in its current spawned nodelet
    	and return to nodelet 0.
        So in total there is one migration on each 0..1, 0..2, ..... 0..7 and another for 
        each 1..0, 2..0, ....., 7..0 the spawned threads on each nodelet migrate back to 0.
        ie. 1..0, ..... ,7..0 

    Cause: 
    	cilk_migrate_hint(rows_ + i) informs the runtime that the next thread should be
        spawned on the nodelet that contains address "rows_ + i".
        So the main thread migrates to that nodelet and then spawns a thread.
    */

    Matrix_t * A = Matrix_t::create(nrows);

    /*
    Resulting Memory Map at this stage
    6675,1,1,1,1,1,1,1
    2,10,0,0,0,0,0,0
    2,0,10,0,0,0,0,0
    2,0,0,10,0,0,0,0
    2,0,0,0,10,0,0,0
    2,0,0,0,0,10,0,0
    2,0,0,0,0,0,10,0
    2,0,0,0,0,0,0,10
    */

    /*
    Matrix B will have 2 rows per nodelet, total 2RowsX8Nodelets
    
    Same expected Migration and Cause as before. Just doubles for the new Matrix
    */

    Matrix_t * B = Matrix_t::create(nrows);
    
    /*
    Resulting Memory Map at this stage
    7246,2,2,2,2,2,2,2
    4,20,0,0,0,0,0,0
    4,0,20,0,0,0,0,0
    4,0,0,20,0,0,0,0
    4,0,0,0,20,0,0,0
    4,0,0,0,0,20,0,0
    4,0,0,0,0,0,20,0
    4,0,0,0,0,0,0,20
    */

    Index_t row_idx_1 = 2; // Build at 1st row in 2nd nodelet
    /*
    Expected Migration:
	The last spawned thread from allocRows migrates to nodelet 2 
        and spawns a build function at nodelet 2.
        Hence there is one additional migration from 0..2 and two
        additional migrations from 2..0
    Cause:
	because of the cilk_migrate_hint at nodelet 2
    */
    cilk_migrate_hint(A->nodelet_addr(row_idx_1));
    cilk_spawn A->build(row_idx_1);
    /*
    Resulting Memory Map at this stage:
    7282,2,3,2,2,2,2,2
    4,20,0,0,0,0,0,0
    6,0,1381,0,0,0,0,0
    4,0,0,20,0,0,0,0
    4,0,0,0,20,0,0,0
    4,0,0,0,0,20,0,0
    4,0,0,0,0,0,20,0
    4,0,0,0,0,0,0,20
   */

    Index_t row_idx_2 = 13; // Build at 2nd row in 5th nodelet
    /*
    Expected Migration:
	The last spawned thread from allocRows migrates to nodelet 5 
        and spawns a build function at nodelet 5
        Hence there is one additional migration from 0..5 and two
        additional migrations from 5..0
    Cause:
	because of the cilk_migrate_hint at nodelet 5
    */
    cilk_migrate_hint(B->nodelet_addr(row_idx_2));
    cilk_spawn B->build(row_idx_2);
    cilk_sync;

    /*
    Resulting Memory Map at end of the program:
    7323,2,3,2,2,3,2,2
    4,20,0,0,0,0,0,0
    6,0,1381,0,0,0,0,0
    4,0,0,20,0,0,0,0
    4,0,0,0,20,0,0,0
    6,0,0,0,0,1338,0,0
    4,0,0,0,0,0,20,0
    4,0,0,0,0,0,0,20
   */

    hooks_region_end();

    return 0;
}

