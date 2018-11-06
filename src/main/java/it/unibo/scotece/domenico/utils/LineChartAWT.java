package it.unibo.scotece.domenico.utils;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class LineChartAWT extends ApplicationFrame {

    /**
     * Constructs a new application frame.
     *
     * @param title the frame title.
     */
    public LineChartAWT(String title, HashMap<Double,Double> values) {
        super(title);

        JFreeChart xylineChart = ChartFactory.createXYLineChart(
                "Probability Distribution" ,
                "Access Frequency (%)" ,
                "Probability" ,
                createDataset(values) ,
                PlotOrientation.VERTICAL ,
                false , true , false);


        ChartPanel chartPanel = new ChartPanel(xylineChart);
        chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
        final XYPlot plot = xylineChart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesPaint(0 , Color.RED);
        renderer.setSeriesStroke(0 , new BasicStroke(2.0f));
        plot.setRenderer(renderer);
        setContentPane(chartPanel);

        int width = 640;   /* Width of the image */
        int height = 480;  /* Height of the image */
        File XYChart = new File("XYLineChart.jpeg");
        try {
            ChartUtilities.saveChartAsJPEG(XYChart, xylineChart, width, height);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private XYDataset createDataset(HashMap<Double,Double> values) {
        final XYSeries prob = new XYSeries("prob");
        final XYSeriesCollection dataset = new XYSeriesCollection();

        for (var val : values.entrySet()){
            prob.add(val.getKey(), val.getValue() );
        }

        dataset.addSeries(prob);
        return dataset;
    }
}
