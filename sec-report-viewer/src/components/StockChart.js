
import { scaleTime } from "d3-scale";
import { format } from "d3-format";

import React from "react";

import { ChartCanvas, Chart } from "react-stockcharts";
import { AreaSeries } from "react-stockcharts/lib/series";
import { XAxis, YAxis } from "react-stockcharts/lib/axes";
import { fitWidth } from "react-stockcharts/lib/helper";

const AreaChartWithYPercent = ({ data, type, width, ratio}) => {
    const getDate = (d) => {
        return d ? d.date : undefined;
    }
    return (<ChartCanvas ratio={ratio} width={width} height={400}
                         margin={{ left: 50, right: 50, top: 10, bottom: 30 }}
                         data={data} type={type}
                         xAccessor={getDate}
                         xScale={scaleTime()}
                         xExtents={[new Date(2010, 0, 1), new Date(2020, 12, 5)]}>
            <Chart id={0} yExtents={d => d.close}>
                <XAxis axisAt="bottom" orient="bottom" ticks={6}/>
                <YAxis axisAt="left" orient="left" />
                <YAxis axisAt="right" orient="right" percentScale={true} tickFormat={format(".0%")}/>
                <AreaSeries yAccessor={d => d.close}/>
            </Chart>
        </ChartCanvas>
    );
};

export const StockChart = fitWidth(AreaChartWithYPercent)
