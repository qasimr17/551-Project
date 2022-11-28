import { Component, OnInit, Input } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import {ApiService} from '../services/api.service';
import {NgToastService} from 'ng-angular-popup';

@Component({
  selector: 'app-add-folder',
  templateUrl: './add-folder.component.html',
  styleUrls: ['./add-folder.component.scss']
})
export class AddFolderComponent implements OnInit {
  path ='';


  constructor(public dialogRef: MatDialogRef<AddFolderComponent>, public api: ApiService, public toast:NgToastService) { }

  ngOnInit(): void {
    console.log("add folder component called")
  }

  // When the user clicks the action button a.k.a. the logout button in the\
  // modal, show an alert and followed by the closing of the modal
  create(name:any) {
    if(this.api.currentPath.length > 1){
      //call mkdir
      this.path = this.api.currentPath+'/'+name;
      this.api.mkdir(this.path).subscribe((response: any) =>{
        if(response['flag']>0){
          this.toast.success({detail :'Success', summary : response.desc, sticky : true, position : 'br'});
        }
        else{
          this.toast.error({detail :'Error', summary : response.desc, sticky : true, position : 'br'}); 
        }
      });
      console.log('Create:'+ this.api.currentPath+'/'+name);
    }
      
    else{
      //call mkdir
      this.path = this.api.currentPath+name;
      this.api.mkdir(this.path).subscribe((response: any) =>{
        if(response['flag']>0){
          this.toast.success({detail :'Success', summary : response.desc, sticky : true, position : 'br'});
        }
        else{
          this.toast.error({detail :'Error', summary : response.desc, sticky : true, position : 'br'}); 
        }
      });
      console.log('Create:'+ this.api.currentPath+name);
    }
      
    this.closeModal();
  }

  // If the user clicks the cancel button a.k.a. the go back button, then\
  // just close the modal
  closeModal() {
    this.dialogRef.close();
  }
}
