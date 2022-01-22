from operator import le
from requests.api import get, post
from requests.models import Response
import scrapy
from scrapy import Request, FormRequest
from scrapy.http.request import form
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json

class Dream_home_realty_Spider(scrapy.Spider):

    name = 'dreamhomerealty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['dreamhomerealty.ca']
    start_urls = ['https://www.dreamhomerealty.ca/aprg/list/mylistingsp.aspx?sid=100602']

    position = 1


    def parse(self, response):

        formdata={
            '__EVENTTARGET':'selListingFor',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS':'' ,
            '__VIEWSTATE':'/wEPDwUJMjA0MjQxNzk5DxYCHhNWYWxpZGF0ZVJlcXVlc3RNb2RlAgEWAgICD2QWLmYPFgIeB1Zpc2libGVoZAIBDxBkZBYBZmQCAg8PFgIeCEltYWdlVXJsBRouLi8uLi9pbWFnZS9hcnJvdy1kb3duLmdpZmRkAgMPDxYCHwIFGC4uLy4uL2ltYWdlL2Fycm93LXVwLmdpZmRkAgQPDxYCHwIFGi4uLy4uL2ltYWdlL2Fycm93LWRvd24uZ2lmZGQCBQ8PFgIfAgUYLi4vLi4vaW1hZ2UvYXJyb3ctdXAuZ2lmZGQCBg8PFgIfAgUaLi4vLi4vaW1hZ2UvYXJyb3ctZG93bi5naWZkZAIHDw8WAh8CBRguLi8uLi9pbWFnZS9hcnJvdy11cC5naWZkZAIKDxYCHgRUZXh0BWA8c3Ryb25nPlRvdGFsPC9zdHJvbmc+OiAyNCAgcHJvcGVydGllcyA8c3BhbiBzdHlsZT0nZm9udC13ZWlnaHQ6Ym9sZDtjb2xvcjpyZWQ7Jz5Gb3IgU2FsZTwvc3Bhbj5kAgsPDxYCHwFoZGQCDA8PFgIfAWhkZAINDw8WAh8DBQZOZXh0ID5kZAIODw8WAh8DBRBUaGUgTGFzdCBQYWdlID4+ZGQCDw8QZBAVAgExATIVAgExATIUKwMCZ2cWAWZkAhAPDxYCHwMFATJkZAIRDxYCHwMFBVBhZ2VzZAITDw8WAh8BaGRkAhQPDxYCHwFoZGQCFQ8PFgIfAwUGTmV4dCA+ZGQCFg8PFgIfAwUQVGhlIExhc3QgUGFnZSA+PmRkAhcPEGQQFQIBMQEyFQIBMQEyFCsDAmdnFgFmZAIYDw8WAh8DBQEyZGQCGQ8WAh8DBQVQYWdlc2QYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgYFDGJ0blNvcnRUeXBlQQUMYnRuU29ydFR5cGVEBQ1idG5Tb3J0UHJpY2VBBQ1idG5Tb3J0UHJpY2VEBQpidG5Tb3J0QmRBBQpidG5Tb3J0QmREPMcwLlHLBREKZtHGr4gRk0UnFIw=',
            '__VIEWSTATEGENERATOR':'6DDF5348',
            '__EVENTVALIDATION':'/wEdABdw1Xf18Y9qfF3YE+Yrd76z30h+ZJp0pNH3CqN+ijTz9K9iSRIHgyi8DSwzjBWJwViAT/6gfAqdK2UgSNIp9J8nA7A1QzJI96QWf22/oJ+dViSGJDYIFolzRAONJUekm5Wci1HtmWnIwdGagKsCDu1CRSLeN093H7NvgvY02kyaAxTtt8sbwgfBal/bQ9U1ypKhLYV0owKg0astpod/Z7wox6FNwiVMGKtFhhKPNS9fGrZ4sdxd7IRwlj+q22XXX9geoqsHr5sAfMuQPOzpJ44mWvLm55E/areq9mGZjtJfpOuq+yQUYpXp/whbjCa0KSgYdugHsdfIPG+UujSE0D9k6lC3a5v+8GD02pgoFjScWcSfNLhubMITe+iXWk6YEobinBTTI1WiiXc/qR2FWx1rnpBO6qIZ7dkYg10k3L6K8XBo2HxGbGPxCM6HanLr7Q3iCDSZXOuJWq8M9nA4EbjatsRC8GE3gY0lMwoL9GTxp3GdFD+hXJ/ulytUzYyxieL9o6+D',
            'selListingFor':'2',
            'txtSF':'10',
            'txtSO':'1',
            'ddlPage':'1',
            'ddlPage1':'1'
            
            }
        for i in range(1,4):
            formdata['ddlPage'] = str(i)
            if i>1:
                formdata['__EVENTTARGET'] = 'ddlPage'
                formdata['__VIEWSTATE']='/wEPDwUJMjA0MjQxNzk5DxYCHhNWYWxpZGF0ZVJlcXVlc3RNb2RlAgEWAgICD2QWLmYPFgIeB1Zpc2libGVoZAIBDxBkZBYBAgFkAgIPDxYCHghJbWFnZVVybAUaLi4vLi4vaW1hZ2UvYXJyb3ctZG93bi5naWZkZAIDDw8WAh8CBRguLi8uLi9pbWFnZS9hcnJvdy11cC5naWZkZAIEDw8WAh8CBRouLi8uLi9pbWFnZS9hcnJvdy1kb3duLmdpZmRkAgUPDxYCHwIFGC4uLy4uL2ltYWdlL2Fycm93LXVwLmdpZmRkAgYPDxYCHwIFGi4uLy4uL2ltYWdlL2Fycm93LWRvd24uZ2lmZGQCBw8PFgIfAgUYLi4vLi4vaW1hZ2UvYXJyb3ctdXAuZ2lmZGQCCg8WAh4EVGV4dAVhPHN0cm9uZz5Ub3RhbDwvc3Ryb25nPjogNDEgIHByb3BlcnRpZXMgPHNwYW4gc3R5bGU9J2ZvbnQtd2VpZ2h0OmJvbGQ7Y29sb3I6cmVkOyc+Rm9yIExlYXNlPC9zcGFuPmQCCw8PFgIfAWhkZAIMDw8WAh8BaGRkAg0PDxYCHwMFBk5leHQgPmRkAg4PDxYCHwMFEFRoZSBMYXN0IFBhZ2UgPj5kZAIPDxBkEBUDATEBMgEzFQMBMQEyATMUKwMDZ2dnFgFmZAIQDw8WAh8DBQEzZGQCEQ8WAh8DBQVQYWdlc2QCEw8PFgIfAWhkZAIUDw8WAh8BaGRkAhUPDxYCHwMFBk5leHQgPmRkAhYPDxYCHwMFEFRoZSBMYXN0IFBhZ2UgPj5kZAIXDxBkEBUDATEBMgEzFQMBMQEyATMUKwMDZ2dnFgFmZAIYDw8WAh8DBQEzZGQCGQ8WAh8DBQVQYWdlc2QYAQUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgYFDGJ0blNvcnRUeXBlQQUMYnRuU29ydFR5cGVEBQ1idG5Tb3J0UHJpY2VBBQ1idG5Tb3J0UHJpY2VEBQpidG5Tb3J0QmRBBQpidG5Tb3J0QmREf362fJB7VvAbCzR+NKMTpMvqKX0='
                formdata['__EVENTVALIDATION']='/wEdABlZT/pg4GFkzORgSFS/G2yX30h+ZJp0pNH3CqN+ijTz9K9iSRIHgyi8DSwzjBWJwViAT/6gfAqdK2UgSNIp9J8nA7A1QzJI96QWf22/oJ+dViSGJDYIFolzRAONJUekm5Wci1HtmWnIwdGagKsCDu1CRSLeN093H7NvgvY02kyaAxTtt8sbwgfBal/bQ9U1ypKhLYV0owKg0astpod/Z7wox6FNwiVMGKtFhhKPNS9fGrZ4sdxd7IRwlj+q22XXX9geoqsHr5sAfMuQPOzpJ44mWvLm55E/areq9mGZjtJfpOuq+yQUYpXp/whbjCa0KSgYdugHsdfIPG+UujSE0D9k6lC3a5v+8GD02pgoFjScWcSfNLhubMITe+iXWk6YEoYVg7XJgOolJGTOFr1ZdAcJ4pwU0yNVool3P6kdhVsda56QTuqiGe3ZGINdJNy+ivFwaNh8Rmxj8QjOh2py6+0N4gg0mVzriVqvDPZwOBG42rbEQvBhN4GNJTMKC/Rk8ad2yeKuYA6S7eaXjTydlumYQmn5i6Zm39HiX7iBeGyT06CDogA='
            yield FormRequest('https://www.dreamhomerealty.ca/aprg/list/mylistingsp.aspx?sid=100602', 
            
            method='POST',
            formdata=formdata,
                callback=self.parseApartment)
        
    def parseApartment(self,response):
        
        apartments = response.css('.text-right a::attr(href)').getall()
        
        apartments = ['https://www.dreamhomerealty.ca/'+x for x in apartments]

        print('='*50)
        print(len(apartments))
        print('='*50)
        if len(apartments)>0:
            for apartment in apartments:
                yield Request(apartment,callback=self.parseDetails)

    def parseDetails(self,response):
        print('='*50)
        print(response.url)
        print('='*50)
        details = response.css(".factsheet .fieldvalue *::text").getall()
        square_meters = 0
        room_count = 1
        bathroom_count = 1
        parking = False
        property_type  = ''
        for idx, val in enumerate(details):
            if 'Condo' in val:
                property_type = 'apartment'
            elif 'Detached' in val or 'Level' in val: 
                property_type = 'house'

            if 'Square' in val:
                details[idx+1] = details[idx+1].lower()
                if 'sq' in details[idx+1]:
                    details[idx+1] = details[idx+1].replace('sq ft',"")
                    details[idx+1] = '0-'+details[idx+1]
                nums = details[idx+1].split('-')
                n1 = int(nums[0])
                n2 = int(nums[1])
                if n1>0:
                    avg = float(((float)(n1)+(float)(n2))/2)/10.764
                else:
                    avg = float((float)(n2))/10.764
                square_meters = int(avg+1)

            '''if 'Land Size' in val and square_meters==0:
                details[idx+1] = details[idx+1].replace('Feet',"").replace(' ','')
                nums = details[idx+1].split('X')
                n1 = float(nums[0])
                n2 = float(nums[1])
                avg = float(((float)(n1)*(float)(n2)))/10.764
                square_meters = int(avg+1)'''

            if '#Bedroom' in val:
                if '+' in details[idx+1]:
                    details[idx+1] = details[idx+1].replace(' ',"")
                    nums = details[idx+1].split('+')
                    room_count = int(nums[0])+int(nums[1])
                else:
                    room_count = int(details[idx+1])

            if '#Bathroom' in val:
                bathroom_count = int(details[idx+1])
            
            if '#Parkings' in val or 'Undergrnd' in val:
                parking = True
            
        if property_type=='':
            return

        external_id = response.css('.propbrief h2::text').get()
        if external_id:
            external_id = remove_white_spaces(external_id)
        else:
            external_id = ''
        
        #description
        description = response.css('.desc::text').get()
        if description:
            description =  remove_white_spaces(description)
        else:
            description=''

        address = response.css(".value.colc::text").get()
        if address:
            address =  remove_white_spaces(address)
        else:
            address = ''
        #Rent
        rentText = response.css(".value.colb:contains('$')::text").get()
        if rentText:
            rex = re.findall(r'\d+\,\d+',rentText)
            if len(rex)>0:
                rent= int(rex[0].replace(',',""))

        print(rent)
        

        

        images = response.css('.thumb a img::attr(src)').getall()
        images = ['https://www.dreamhomerealty.ca/'+x for x in images]

	    
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']

        longitude  = str(longitude)
        latitude  = str(latitude)


  
        
        if rent>0:

            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", external_id)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", int(int(square_meters)*10.764))
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "CAD")
            #item_loader.add_value("energy_label", energy_label)
            #item_loader.add_value("furnished", furnished)
            #item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            #item_loader.add_value("elevator", elevator)
            #item_loader.add_value("balcony", balcony)
            
            #item_loader.add_value("terrace", terrace)
            item_loader.add_value("landlord_name", 'Dream Home Realty')
            item_loader.add_value("landlord_email", 'info@dreamhomerealty.ca')
            item_loader.add_value("landlord_phone", '905-604-6855')
            item_loader.add_value("position", self.position)

            self.position+=1

            
            yield item_loader.load_item()