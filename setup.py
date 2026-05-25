from setuptools import find_packages, setup
 
package_name = "gui_image_streamer"
 
setup(
    name=package_name,
    version="1.0.0",
    packages=find_packages(exclude=["test"]),
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="user",
    maintainer_email="user@robot.com",
    description="WebSocket image streamer for GUI (Python)",
    license="MIT",
    entry_points={
        "console_scripts": [
            # ros2 run gui_image_streamer image_streamer
            "image_streamer = gui_image_streamer.image_streamer:main",
        ],
    },
)
 