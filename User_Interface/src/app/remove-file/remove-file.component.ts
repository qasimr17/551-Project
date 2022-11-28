import { Component, OnInit, Input } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import { ApiService } from '../services/api.service';
import {NgToastService} from 'ng-angular-popup';

@Component({
  selector: 'app-remove-file',
  templateUrl: './remove-file.component.html',
  styleUrls: ['./remove-file.component.scss']
})
export class RemoveFileComponent implements OnInit {

  children: String[] = [];
  currentPath = '';
  selectedFile = '';
  constructor(public dialogRef: MatDialogRef<RemoveFileComponent>, public api: ApiService, public toast: NgToastService) { }

  ngOnInit(): void {
    this.children = this.api.children;
    let files = [];
    for(let child of this.children){
      if(child.split('.')[1]){
        files.push(child);
      }
    }
    this.children = files;
    this.currentPath = this.api.currentPath;
    if(this.currentPath.length>1){
      this.currentPath = this.currentPath + '/';
    }
      
  }

  // When the user clicks the action button a.k.a. the logout button in the\
  // modal, show an alert and followed by the closing of the modal
  rm() {
    this.api.rm(this.currentPath+this.selectedFile).subscribe((response: any) =>{

      if(response['flag']>0){
        this.toast.success({detail :'Success', summary : response.desc, sticky : true, position : 'br'});
      }
      else{
        this.toast.error({detail :'Error', summary : response.desc, sticky : true, position : 'br'}); 
      }
      this.dialogRef.close();
    });
  }

  // If the user clicks the cancel button a.k.a. the go back button, then\
  // just close the modal
  closeModal() {
    this.dialogRef.close();
  }

}
