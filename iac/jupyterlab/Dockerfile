FROM fiores/spark:base-v3.4

USER root

RUN pip3 install --no-cache-dir jupyterlab traitlets pygments s3contents \
	&& mkdir -p /home/public && chmod 777 /home/public
RUN pip3 install pyspark
RUN pip3 install jupyterlab-sparkmonitor ipynb
RUN pip3 install jupyterlab-s3-browser bokeh
RUN jupyter serverextension enable --py jupyterlab_s3_browser
RUN pip3 install sparksql-magic
RUN ipython profile create && \
echo "c.InteractiveShellApp.extensions.append('sparkmonitor.kernelextension')" >>  $(ipython profile locate default)/ipython_kernel_config.py

RUN ln -s /usr/local/lib/python3.8/site-packages/sparkmonitor/listener_2.12.jar /opt/spark/jars/listener_2.12.jar
