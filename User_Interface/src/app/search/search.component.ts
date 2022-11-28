import { Component, OnInit, Input } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import {ApiService} from '../services/api.service';
import { FormArray, FormControl, FormGroup } from '@angular/forms';
import {faSmileWink, faX} from '@fortawesome/free-solid-svg-icons';
import {NgxSpinnerService} from 'ngx-spinner';

interface columnFilter{
  column: String,
  operator: String,
  value: String,
  condition: String
}
interface operator{
  name: String,
  value: String
}

interface apiColumnFilter{
  column : {}
}

@Component({
  selector: 'app-search',
  templateUrl: './search.component.html',
  styleUrls: ['./search.component.scss']
})
export class SearchComponent implements OnInit {
  columns: any[] = [];
  selectedColumns: any[]= [];
  aggregators = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX'];
  selectedAggregator = null;
  selectedAggregatorColumn = null;
  columnFilters: columnFilter[] = [];
  operators: operator[] = [{name:'lessThan', value: '<'},{name:'greaterThan', value: '>'},{name:'equals', value: '='},{name:'lessThanEqualTo', value: '<='},{name:'greaterThanEqualTo', value: '>='},{name:'notEqualTo', value: '!='}];
  selectedOperator = null;
  selectedValue = null;
  selectedFilterColumn = '';
  selectedDistinctColumns = [];
  selectedGroupByColumns = [];
  parameters = {};
  showData = false;
  jsonData : any;
  data : any
  faCross = faX;
  conditions = ['and', 'or'];
  currentPath = '';
  finalTable = false;
  intermediateTable = false;
  displayExplain = false;
  final: any;
  inter: any;
  finalData: any;
  interData: any

  // sample = {"intermediate_message": [{"message": "The max value for partition p0 is: ", "value": "15"}, {"message": "The max value for partition p1 is: ", "value": "18"}, {"message": "The max value for partition p2 is: ", "value": "23"}, {"message": "The max value for partition p3 is: ", "value": "12"}, {"message": "The max value for partition p4 is: ", "value": "23"}], "final_result": {"message": "The final output is a max of the intermediate results: ", "value": "23"}}


  // sample = {
  //   "intermediate_message": [{
  //       "message": "Top 3 rows for p0 are: ",
  //       "value": "{\"schema\":{\"fields\":[{\"name\":\"index\",\"type\":\"integer\"},{\"name\":\"team_code\",\"type\":\"integer\"}],\"primaryKey\":[\"index\"],\"pandas_version\":\"1.4.0\"},\"data\":[{\"index\":0,\"team_code\":3},{\"index\":10,\"team_code\":7},{\"index\":18,\"team_code\":94}]}"
  //     },
  //     {
  //       "message": "Top 3 rows for p1 are: ",
  //       "value": "{\"schema\":{\"fields\":[{\"name\":\"index\",\"type\":\"integer\"},{\"name\":\"team_code\",\"type\":\"integer\"}],\"primaryKey\":[\"index\"],\"pandas_version\":\"1.4.0\"},\"data\":[{\"index\":0,\"team_code\":3},{\"index\":9,\"team_code\":7},{\"index\":17,\"team_code\":94}]}"
  //     },
  //     {
  //       "message": "Top 3 rows for p2 are: ",
  //       "value": "{\"schema\":{\"fields\":[{\"name\":\"index\",\"type\":\"integer\"},{\"name\":\"team_code\",\"type\":\"integer\"}],\"primaryKey\":[\"index\"],\"pandas_version\":\"1.4.0\"},\"data\":[{\"index\":0,\"team_code\":3},{\"index\":9,\"team_code\":7},{\"index\":17,\"team_code\":94}]}"
  //     },
  //     {
  //       "message": "Top 3 rows for p3 are: ",
  //       "value": "{\"schema\":{\"fields\":[{\"name\":\"index\",\"type\":\"integer\"},{\"name\":\"team_code\",\"type\":\"integer\"}],\"primaryKey\":[\"index\"],\"pandas_version\":\"1.4.0\"},\"data\":[{\"index\":0,\"team_code\":3},{\"index\":9,\"team_code\":7},{\"index\":17,\"team_code\":94}]}"
  //     },
  //     {
  //       "message": "Top 3 rows for p4 are: ",
  //       "value": "{\"schema\":{\"fields\":[{\"name\":\"index\",\"type\":\"integer\"},{\"name\":\"team_code\",\"type\":\"integer\"}],\"primaryKey\":[\"index\"],\"pandas_version\":\"1.4.0\"},\"data\":[{\"index\":0,\"team_code\":3},{\"index\":9,\"team_code\":7},{\"index\":17,\"team_code\":94}]}"
  //     }
  //   ],
  //   "final_result": {
  //     "message": "The final, complete df is: ",
  //     "value": "{\"schema\":{\"fields\":[{\"name\":\"index\",\"type\":\"integer\"},{\"name\":\"team_code\",\"type\":\"integer\"}],\"primaryKey\":[\"index\"],\"pandas_version\":\"1.4.0\"},\"data\":[{\"index\":0,\"team_code\":3},{\"index\":1,\"team_code\":7},{\"index\":2,\"team_code\":94},{\"index\":3,\"team_code\":36},{\"index\":4,\"team_code\":90},{\"index\":5,\"team_code\":8},{\"index\":6,\"team_code\":31},{\"index\":7,\"team_code\":11},{\"index\":8,\"team_code\":13},{\"index\":9,\"team_code\":2},{\"index\":10,\"team_code\":14},{\"index\":11,\"team_code\":43},{\"index\":12,\"team_code\":1},{\"index\":13,\"team_code\":4},{\"index\":14,\"team_code\":45},{\"index\":15,\"team_code\":20},{\"index\":16,\"team_code\":6},{\"index\":17,\"team_code\":57},{\"index\":18,\"team_code\":21},{\"index\":19,\"team_code\":39}]}"
  //   }
  // }

  constructor(public dialogRef: MatDialogRef<SearchComponent>, public api: ApiService, public spinner: NgxSpinnerService) { }

  ngOnInit(): void {
    this.columns = this.api.columns;
    this.columnFilters.push({
      column : '',
      operator : '',
      value : '',
      condition :''
    });
    this.currentPath = this.api.currentPath;
  }

  addFilter(){
    this.columnFilters.push({
      column: '',
      operator: '',
      value: '',
      condition: ''
    });
  }

  // When the user clicks the action button a.k.a. the logout button in the\
  // modal, show an alert and followed by the closing of the modal
  search() {
    // console.log(this.selectedColumns)
    // console.log(this.selectedAggregator);
    // console.log(this.selectedAggregatorColumn);
    // console.log(this.selectedFilterColumn)
    // console.log(this.selectedOperator)
    // console.log(this.selectedValue)
    // console.log(this.selectedDistinctColumns);
    // console.log(this.selectedGroupByColumns);

    if(!this.columnFilters[0].column){
      this.columnFilters.pop()
    }
    
    this.columns = [];
    this.jsonData = null;
    this.data = null;
    this.parameters = {
      "filePath": this.api.currentPath,
      "aggregatorFunction": this.selectedAggregator,
      "aggregatorColumn": this.selectedAggregatorColumn,
      "columnFilters": this.columnFilters,
      "displayColumns": this.selectedColumns,
      "distinctCols": this.selectedDistinctColumns,
      "groupedBy": this.selectedGroupByColumns
    }
    console.log(this.parameters);
    // this.jsonData = JSON.parse('{"schema":{"fields":[{"name":"index","type":"integer"},{"name":"aaa","type":"integer"},{"name":"bbb","type":"string"},{"name":"ccc","type":"string"},{"name":"ddd","type":"integer"}],"primaryKey":["index"],"pandas_version":"1.4.0"},"data":[{"index":0,"aaa":1,"bbb":"qwe","ccc":"asd","ddd":1234},{"index":1,"aaa":2,"bbb":"wer","ccc":"asd","ddd":23454},{"index":2,"aaa":3,"bbb":"ert","ccc":"sdf","ddd":34566},{"index":3,"aaa":4,"bbb":"rty","ccc":"dfg","ddd":456},{"index":4,"aaa":5,"bbb":"tyu","ccc":"fgh","ddd":4677},{"index":0,"aaa":1,"bbb":"qwe","ccc":"asd","ddd":1234},{"index":1,"aaa":2,"bbb":"wer","ccc":"asd","ddd":23454},{"index":2,"aaa":3,"bbb":"ert","ccc":"sdf","ddd":34566},{"index":3,"aaa":4,"bbb":"rty","ccc":"dfg","ddd":456},{"index":4,"aaa":5,"bbb":"tyu","ccc":"fgh","ddd":4677},{"index":0,"aaa":1,"bbb":"qwe","ccc":"asd","ddd":1234},{"index":1,"aaa":2,"bbb":"wer","ccc":"asd","ddd":23454},{"index":2,"aaa":3,"bbb":"ert","ccc":"sdf","ddd":34566},{"index":3,"aaa":4,"bbb":"rty","ccc":"dfg","ddd":456},{"index":4,"aaa":5,"bbb":"tyu","ccc":"fgh","ddd":4677}]}');
    // this.api.refreshData(this.data);
    this.api.executeQuery(this.currentPath, this.parameters).subscribe((response) => {
      // this.spinner.show();
      this.jsonData = response;
      this.showData= true;

    this.final = this.jsonData['final_result']
    this.inter = this.jsonData['intermediate_message']
    this.finalData = JSON.parse(this.final['value'])
    
    if(this.finalData['data']){
      console.log("final is a table")
      this.finalTable = true;
      // this.finalData = this.finalData['data']
      console.log(this.finalData);
      for(let x of this.finalData['schema']['fields']){
        this.columns.push(x['name']);
      }
    }
      // for(let x of this.jsonData['schema']['fields']){
      //   this.columns.push(x['name']);
      // }
      // console.log(this.columns)
      // this.data =this.jsonData['data'];
      // this.showData = true;
      // this.spinner.hide();
    });
    //this is working with hardcoded sample
    // this.jsonData = this.sample;
    // this.showData= true;

    // this.final = this.jsonData['final_result']
    // this.inter = this.jsonData['intermediate_message']
    // this.finalData = JSON.parse(this.final['value'])
    
    // if(this.finalData['data']){
    //   console.log("final is a table")
    //   this.finalTable = true;
    //   // this.finalData = this.finalData['data']
    //   console.log(this.finalData);
    //   for(let x of this.finalData['schema']['fields']){
    //     this.columns.push(x['name']);
    //   }
    // }

    

  }
  getJsonData(data: any){
    let result = JSON.parse(data)
    console.log(result)
    return result['data'];
  }
  explain(){
    this.displayExplain = true;
    // if(this.final['data']){
    //   this.finalTable = true;
    //   console.log("explain final is a table")
    // }
    if(this.inter[0]['value'] && this.getJsonData(this.inter[0]['value'])){
      this.intermediateTable = true;
      console.log("explain intermediate is a table")
    }

  }

  // If the user clicks the cancel button a.k.a. the go back button, then\
  // just close the modal
  closeModal() {
    this.dialogRef.close();
  }

}
