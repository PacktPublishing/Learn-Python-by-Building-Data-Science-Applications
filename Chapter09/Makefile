.PHONY: test upload

test:
	echo 'testing!'

upload:
	aws s3 cp ./ s3://mybucket/wikiwwii --recursive --dryrun
