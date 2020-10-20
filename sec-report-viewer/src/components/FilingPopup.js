import Dialog from "@material-ui/core/Dialog";
import React, {useEffect, useMemo, useState} from "react";
import DialogTitle from "@material-ui/core/DialogTitle";
import styled from "styled-components";
import TextField from "@material-ui/core/TextField";
import {Tabs, Tab} from "@material-ui/core";
import {location} from "../commons";
import {cellWithTwoValues, formatMetric, HeaderRow, PagedGrid, SimpleGrid} from "./table";
import {Title} from "@material-ui/icons";
import Typography from "@material-ui/core/Typography";

export const FilingAmendmentPopup = ({filing, onClose}) => {
    const [open, setOpen] = useState(true)
    const close = () => {
        setOpen(false)
        onClose()
    }

    return (<Dialog open={open} maxWidth="lg" onClose={close}>
                <DialogTitle id="simple-dialog-title">Amend {filing.ticker}</DialogTitle>
                <Content className="content">
                    <AmendmentView filing={filing}/>
                    <CommentSection comments={[]}/>
                </Content>
            </Dialog>)
}

function a11yProps(index) {
    return {
        id: `condensed-report-${index}`,
        'aria-controls': `condensed-report-${index}`,
    };
}

const CondensedReport = ({type, filingId}) => {
    const [data, setData] = useState([])
    useEffect(() => {
        async function loadData() {
            let fetchedFilings = await fetch(`${location}/condensedReports?type=${type}&id=${filingId}`, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            let json = [];
            try {
                json = await fetchedFilings.json()
            } catch(e) {
                json = []
            }

            setData(json)
        }

        loadData()
    }, [type]);

    let grid = <span>No data</span>;
    const columns = useMemo(() => condensedReportColumns("", type), [])

    if (data.sections) {
        const allRows = data.sections
            .filter(section => section.rows.length > 0)
            .flatMap(section => [ {label: section.label, isSection: true}, ...section.rows])

        grid = (<SimpleGrid data={allRows} columns={columns}/>)
    }

    return (<div role="condensed-report">
        {grid}
    </div>)
}

// onChange={handleChange}
const CondensedReports = ({filing}) => {
    const [selected, setSelected] = useState(0)
    const types = ["balance", "operations", "cashflow", "income"]
    const currentType = selected ? types[selected] : types[0];

    const updateSelected = (e, value) => {
        setSelected(value)
    }

    return (<>
        <Tabs value={selected} onChange={updateSelected}>
            <Tab label="Balance" {...a11yProps(0)} />
            <Tab label="Operations" {...a11yProps(1)} />
            <Tab label="Cashflow" {...a11yProps(2)} />
            <Tab label="Income" {...a11yProps(3)} />
        </Tabs>
        <CondensedReport type={currentType} filingId={filing.id}/>
    </>)
}

const AmendmentView = ({filing}) => {
    return (<CondensedReports filing={filing}/>)
}

const CommentSection = ({comments}) => {
    return (<>
        {comments.map(comment => (<div>{comment}</div>))}
        <TextField
            id="new-comment"
            label="Post comment"
            placeholder="Write something"
            multiline
            variant="outlined"/>
    </>)
}

const condensedReportColumns = (title, period) =>
        [
            {
                Header: 'Title',
                accessor: row => row,
                Cell: ({value}) => <PropertyLabel property={value}/>
            },
            {
                Header: 'Value',
                accessor: row => {
                    return row.values ? row.values[0].value : ""
                },
            },
        ];

const PropertyLabel = ({property}) => {
    const {label, propertyId, isSection} = property
    const renderedLabel = isSection ? <b>{label}</b> : label;
    return <span>
        <span  style={{paddingRight: "0.2em"}}>{renderedLabel}</span>
        <span style={{color: "rgba(224, 224, 224, 1)"}}>{propertyId}</span>
    </span>
}

const Content = styled.div`
        width: 1000px;
        height: 700px;
`;
