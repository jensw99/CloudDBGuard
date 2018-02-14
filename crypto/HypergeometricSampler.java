package crypto;

public interface HypergeometricSampler<T> {
	public T sample(T KK, T NN1, T NN2, OPE_Boldyreva_blockrng prng) throws Exception;
	
}
