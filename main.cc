/*
  MM = MEMORY MAP
  Memory Reference Map. Entry (i,j) is the number of references
  made by threads on nodelet i to nodelet j. i == j for local references.
  Sum of column j is number of references on nodelet j, excluding remotes.
  Sum of row i, excluding i == j, is number of migrations away from nodelet i.

  Each row in the memory map represents the source nodelet on which a memory
  instruction was encountered and the column represents the destination nodelet
  on which the memory instruction was ultimately executed. Values on the
  diagonal represent local memory accesses and non-diagonal values show the
  number of memory accesses that required a migration to another nodelet to
  complete.

  RM = REMOTES MAP
  Remotes Reference Map. Counts number of remote operations from
  threads on nodelet i to nodelet j.

  Each row in the remotes map is a source nodelet and each column is a
  destination nodelet for a remote memory operation. Note that the diagonal
  values are always zero because a remote update to a local value will be
  counted as a local memory operation. In this example, all the values are zero
  showing that no remote memory operations occurred.
 */

#include <string>
#include <vector>

#include <cilk.h>
#include <memoryweb.h>
extern "C" {
#include <emu_c_utils/layout.h>
}

typedef long Index_t;
typedef long Scalar_t;
typedef std::vector<Scalar_t> Vec_t;
typedef Vec_t * pVec_t;

static inline
Index_t r_map(Index_t i) { return i / NODELETS(); } // slow running index
static inline
Index_t n_map(Index_t i) { return i % NODELETS(); } // fast running index
// view-2 pointer to nodelet holding row i
static inline
Vec_t ** nodelet_addr(Vec_t ** v, Index_t i) { return v + n_map(i); }

void print_emu_ptr(std::string name, void * r)
{
    emu_pointer pchk = examine_emu_pointer(r);

    if (pchk.view == 2)
    {
        printf("emu ptr: %s view: %ld\n", name.c_str(), pchk.view);
    }
    else
    {
        printf("emu ptr: %s view: %ld, node_id: %ld, nodelet_id: %ld, "
               "nodelet_addr: %ld, byte_offset: %ld\n",
               name.c_str(),
               pchk.view, pchk.node_id, pchk.nodelet_id, pchk.nodelet_addr,
               pchk.byte_offset);
    }
}

Vec_t ** allocVecs(Index_t nvecs, Index_t nvecs_per_nodelet)
{
    Vec_t ** v
        = (Vec_t **)mw_malloc2d(NODELETS(),
                                nvecs_per_nodelet * sizeof(Vec_t));

    for (Index_t vec_idx = 0; vec_idx < nvecs; ++vec_idx)
    {
        size_t nid(n_map(vec_idx));
        size_t rid(r_map(vec_idx));

        // migrations to do placement new on other nodelets
        pVec_t vecPtr = new(v[nid] + rid) Vec_t();
    }

    return v;
}

void pushBack(Vec_t ** v, Index_t row_idx)
{
    pVec_t vecPtr = v[n_map(row_idx)] + r_map(row_idx);

    for (Index_t i = 0; i < 200; ++i)
    {
        // causes migrations to nodelet 0
        vecPtr->push_back(1);
    }

    // shows that the pushed back data does live on nodelet 7
    /*
      print_emu_ptr("vecPtr", vecPtr);
      print_emu_ptr("data 0", &vecPtr[0]);
      print_emu_ptr("data 1", &vecPtr[1]);
      print_emu_ptr("data 2", &vecPtr[2]);
    */

}

int main(int argc, char* argv[])
{
    starttiming();

    Index_t nvecs = 16;
    Index_t nvecs_per_nodelet = nvecs + nvecs % NODELETS();

    Vec_t ** v = cilk_spawn allocVecs(nvecs, nvecs_per_nodelet);
    cilk_sync;

    // push_back will have a stack, need it to be created at target nodelet
    cilk_migrate_hint(nodelet_addr(v, 15));
    cilk_spawn pushBack(v, 15);
    cilk_sync;

    return 0;
}


/*
  yes, they return a view-2 "striped" pointer, as opposed to a view-1
  "absolute" or view-0 "relative" pointer. We reserve some of the high bits
  to store the view number. The hardware checks this and uses it to determine
  where to pull the nodelet bits from. View-2 is implemented by moving the
  nodelet ID lower in the pointer, so standard pointer arithmetic actually
  changes the nodelet ID.
  and I have to be pedantic here because it's easy to get confused.
  `ptr + 1` _refers_ to a location on nodelet 1, but the pointer itself could
  be stored elsewhere.

  Right, all of this is to help me understand the trick with replicating the
  base pointer. In that case, `ptr` is an address on, say, nodelet 0
  (presumably also stored at an address on nodelet 0) and `ptr + 1` is thereby
  "stored" on nodelet 0, but resulting address is on nodelet 1. Easy to get
  confused indeed

  right so lets apply this to `mw_malloc2D`

  `long ** ptr = mw_malloc2d(NODELETS(), 10 * sizeof(long));` Let's assume 8
  nodelets for simplicity. So we have a striped array of pointers to arrays
  on each nodelet.
  - `&ptr` is a view-1 pointer to to the local variable ptr, which is on the
  current nodelet (probably nodelet 0)
  - `ptr` is view-2
  - `&ptr[2]` is a view-1 pointer that refers to nodelet 2.
  - `ptr[2]` could be rewritten as `*(ptr + 2)`, during evaluation we will
  migrate to nodelet 2 to read the view-1 pointer stored there
  - `ptr[2][3]` We migrate to nodelet 2 to read the view-1 pointer stored on
  nodelet 2 at `&ptr[2]`, then index again to reach a location in the block on
  nodelet 2
  so getting back to replication, we want to be able to start the process from
  any nodelet without migrating back to nodelet 0 where `ptr` is stored. So
  make `&ptr` be view-0 using `replicated`.
*/
