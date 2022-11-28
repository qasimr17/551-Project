import { Component, OnInit, Input } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import { ApiService } from '../services/api.service';
import {NgToastService} from 'ng-angular-popup';

@Component({
  selector: 'app-add-file',
  templateUrl: './add-file.component.html',
  styleUrls: ['./add-file.component.scss']
})
export class AddFileComponent implements OnInit {

  file: any;
  currentPath = '';
  fileName = '';
  constructor(public dialogRef: MatDialogRef<AddFileComponent>, public api: ApiService, public toast:NgToastService) { }

  ngOnInit(): void {
    this.currentPath = this.api.currentPath;
  }

  // When the user clicks the action button a.k.a. the logout button in the\
  // modal, show an alert and followed by the closing of the modal
  create(name: String, no: String, field: String) {
    console.log(name, no, field);
    let data: any;
    if (this.api.currentPath.length > 1) {
      //call mkdir
      console.log('Create:' + this.api.currentPath + '/' + this.file['name']);
      let formData = new FormData();
      formData.append('file',this.file);
      const body = {file: this.file}
      console.log(formData);
      let path = this.api.currentPath + '/' + this.file['name']
      this.api.put(path, no, field,formData).subscribe((response: any) => {
        if(response['flag']>0){
          this.toast.success({detail :'Success', summary : response.desc, sticky : true, position : 'br'});
        }
        else{
          this.toast.error({detail :'Error', summary : response.desc, sticky : true, position : 'br'}); 
        }
      });
      //let fileReader = new FileReader();
      //fileReader.onload = (e) => {
        //console.log(fileReader.result);
        //data = fileReader.result
        //console.log(typeof data);
        // let fileJson = JSON.parse(data);
        
      //}
      //fileReader.readAsText(this.file);
      
      console.log(this.file);
      
    }

    else{
      let formData = new FormData();
      formData.append('file',this.file);
      const body = {file: this.file}
      console.log(formData);
      console.log('Create:' + this.api.currentPath + this.file['name']);
      let path = this.api.currentPath + this.file['name'];
      this.api.put(path, no, field,formData).subscribe((response: any) => {
        if(response['flag']>0){
          this.toast.success({detail :'Success', summary : response.desc, sticky : true, position : 'br'});
        }
        else{
          this.toast.error({detail :'Error', summary : response.desc, sticky : true, position : 'br'}); 
        }
      });
    }
      //call mkdir
     
    this.closeModal();
  }

  // If the user clicks the cancel button a.k.a. the go back button, then\
  // just close the modal
  closeModal() {
    this.dialogRef.close();
  }

  upload(event: any) {
    this.file = null;
    if (event.target.files && event.target.files.length > 0) {
      this.file = event.target.files[0];
      console.log(this.file['name']);
    }
  }

}