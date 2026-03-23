// Quick diagnostic: what is our exact sum for FRANCE 1997?
// Replace %.2Lf with %.6Lf to see more decimal places
#include <cstdio>
int main() {
    // The reference shows 458070785.3050 (4 decimal)
    // Our output shows 458070785.30 (2 decimal)
    // Let's check what our actual long double sum is
    long double rev  = 0.0L; // we need to run the query with more precision
    long double cost = 0.0L;
    // Can't do this without running the query - use inline modification instead
    printf("Reference: 458070785.3050 -> python round -> 458070785.31\n");
    printf("Ours: 458070785.30xx where xx < 50\n");
    // The gap: reference double = 458070785.305 + epsilon
    // Ours double = 458070785.305 - epsilon  
    double ref = 458070785.3050;
    printf("ref as double = %.15f\n", ref);
    printf("ref rounded = %.2f\n", ref);  // should be .31 since double rep > .305
    
    double our_est = 458070785.30499;
    printf("our_est rounded = %.2f\n", our_est);
    return 0;
}
