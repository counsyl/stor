from setuptools import setup
# import sys

setup(
    pbr=True,
    setup_requires=['pbr'],
    # entry_points="""
    #     [{}.stor.providers]
    #     dx = stor_dx:find_cls_for_path
    # """.format(sys.version_info[0])
)
