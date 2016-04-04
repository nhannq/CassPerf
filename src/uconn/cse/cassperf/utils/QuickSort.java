/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

/**
 *
 * @author nhannq
 */
public class QuickSort {
    // quicksort the array

    public static void sort(double[] a, int[] cs, int length) {
        //StdRandom.shuffle(a);
        sort(a, cs, 0, length - 1);
    }

    // quicksort the subarray from a[lo] to a[hi]
    private static void sort(double[] a, int[] cs, int lo, int hi) {
        if (hi <= lo) {
            return;
        }
        int j = partition(a, cs, lo, hi);
        sort(a, cs, lo, j - 1);
        sort(a, cs, j + 1, hi);
//        assert isSorted(a, cs. lo, hi);
    }

    // partition the subarray a[lo .. hi] by returning an index j
    // so that a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
    private static int partition(double[] a, int[] cs, int lo, int hi) {
        int i = lo;
        int j = hi + 1;
        Comparable v = a[cs[lo]];
        while (true) {

            // find item on lo to swap
            while (less(a[cs[++i]], v)) {
                if (i == hi) {
                    break;
                }
            }

            // find item on hi to swap
            while (less(v, a[cs[--j]])) {
                if (j == lo) {
                    break;      // redundant since a[lo] acts as sentinel
                }
            }
            // check if pointers cross
            if (i >= j) {
                break;
            }

            exch(a, cs, i, j);
        }

        // put v = a[j] into position
        exch(a, cs, lo, j);

        // with a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
        return j;
    }

    /**
     * *********************************************************************
     * Rearranges the elements in a so that a[k] is the kth smallest element,
     * and a[0] through a[k-1] are less than or equal to a[k], and a[k+1]
     * through a[n-1] are greater than or equal to a[k].
     * *********************************************************************
     */
//    public static Comparable select(double[] a, int k) {
//        if (k < 0 || k >= a.length) {
//            throw new RuntimeException("Selected element out of bounds");
//        }
////        StdRandom.shuffle(a);
//        int lo = 0, hi = a.length - 1;
//        while (hi > lo) {
//            int i = partition(a, lo, hi);
//            if      (i > k) hi = i - 1;
//            else if (i < k) lo = i + 1;
//            else return a[i];
//        }
//        return a[lo];
//    }
    /**
     * *********************************************************************
     * Helper sorting functions
     * *********************************************************************
     */
    // is v < w ?
    private static boolean less(Comparable v, Comparable w) {
        return (v.compareTo(w) < 0);
    }

    // exchange a[i] and a[j]
    private static void exch(double[] a, int[] cs, int i, int j) {
        int swap = cs[i];
        cs[i] = cs[j];
        cs[j] = swap;
    }

    /**
     * *********************************************************************
     * Check if array is sorted - useful for debugging
     * *********************************************************************
     */
    private static boolean isSorted(double[] a, int[] cs) {
        return isSorted(a, cs, 0, a.length - 1);
    }

    private static boolean isSorted(double[] a, int[] cs, int lo, int hi) {
        for (int i = lo + 1; i <= hi; i++) {
            if (less(a[cs[i]], a[cs[i - 1]])) {
                return false;
            }
        }
        return true;
    }

    // print array to standard output
    public static void show(double[] a, int[] cs) {
        for (int i = 0; i < a.length; i++) {
            System.out.println(cs[i] + " " + a[i]);
        }
    }
}
