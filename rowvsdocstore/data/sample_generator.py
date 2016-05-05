
def generate_sample(size, percent):
	file_src = open('product_reviews.dat', 'r')
	file_sammple = open('product_reviews_'+str(percent) +'.dat', 'w')
	for i in range(size):
		file_sammple.write(file_src.readline())
		i = i+1

	file_sammple.close()
	file_src.close()


if __name__ == '__main__':
	print('generating sample 1%')
	generate_sample(900,1)
	print('generating sample 10%')
	generate_sample(9000,10)
	print('generating sample 25%')
	generate_sample(22500,25)
	print('generating sample 50%')
	generate_sample(45000,50)
	print('generating sample 75%')
	generate_sample(67500,75)
	print('close')

