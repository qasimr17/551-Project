import { Component, OnInit, Input } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import {ApiService} from '../services/api.service';
import {faX} from '@fortawesome/free-solid-svg-icons'


@Component({
  selector: 'app-view-partition',
  templateUrl: './view-partition.component.html',
  styleUrls: ['./view-partition.component.scss']
})
export class ViewPartitionComponent implements OnInit {
  partitions: String[] = [];
  selectedPartition = '';
  jsonData : any;
  data: any;
  showData = false;
  columns: any[] = [];
  faCross = faX;
  currentPath = '';
  parameters = '';

  constructor(public dialogRef: MatDialogRef<ViewPartitionComponent>, public api: ApiService) { }

  ngOnInit(): void {
    //call get partitions
    this.currentPath = this.api.currentPath;
    this.api.getPartitionLocations(this.currentPath).subscribe((response: any)=>{
      this.partitions = response['partitions'];
    });

  }

  // When the user clicks the action button a.k.a. the logout button in the\
  // modal, show an alert and followed by the closing of the modal
  view() {
    //call get parttion
    this.parameters = this.currentPath+'&partition='+this.selectedPartition;
    this.api.readPartition(this.parameters).subscribe((response: any) =>{
      this.jsonData = response;
      for (let x of this.jsonData['schema']['fields']) {
        this.columns.push(x['name']);
      }
      console.log(this.columns)
      this.data = this.jsonData['data'];
      this.showData = true;
    });
  }

  // If the user clicks the cancel button a.k.a. the go back button, then\
  // just close the modal
  closeModal() {
    if(this.showData){
      this.showData = false;
      this.columns = [];
      this.jsonData = null;
      this.data = null;
      this.selectedPartition = ''
    }
    this.dialogRef.close();
  }

}
