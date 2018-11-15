from setuptools import setup
# import sys

setup(
    pbr=True,
    setup_requires=['pbr'],
    # entry_points="""
    #     [stor.providers.{}]
    #     s3 = stor_s3:find_cls_for_path
    # """.format(sys.version_info[0])
)
