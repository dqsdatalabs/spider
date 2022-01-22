# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import json
import re
from ..loaders import ListingLoader

class OptimumlettingsSpider(scrapy.Spider):
    name = "optimumlettings"
    allowed_domains = ["optimumlettings.co.uk"]
    start_urls = (
        'http://www.optimumlettings.co.uk/',
    ) 
    execution_type = 'testing'
    country = 'united_kingdom'
    external_source = "Optimumlettings_PySpider_united_kingdom_en"
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.rightmove.co.uk/api/_search?locationIdentifier=REGION%5E87490&numberOfPropertiesPerPage=24&radius=40.0&sortType=6&index=0&secondaryDisplayPropertyType=parksandmobilehomes&maxDaysSinceAdded=1&includeLetAgreed=false&viewType=LIST&furnishTypes=furnished&letType=longTerm&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=true&propertyTypes=flat', 'property_type': 'apartment'},
            {'url': 'https://www.rightmove.co.uk/api/_search?locationIdentifier=REGION%5E87490&numberOfPropertiesPerPage=24&radius=40.0&sortType=6&index=0&secondaryDisplayPropertyType=detachedshouses&maxDaysSinceAdded=1&includeLetAgreed=false&viewType=LIST&furnishTypes=furnished&letType=longTerm&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=true&propertyTypes=park-home', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        data_json = json.loads(response.text)
        total_pages = data_json['pagination']['total']
        pages = int(total_pages) + 1
        for page_number in range(0, pages):
            page = int(page_number) * 24 
            link = response.url.replace('index=0', 'index={}'.format(page))
            yield scrapy.Request(url=link, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        
        # parse details of the propery
        removed = response.xpath("//p[contains(., 'removed')]/text()").get()
        if removed:
            return
        
        property_details = json.loads(response.text)


        for property_detail in property_details['properties']: 
            external_link = response.urljoin(property_detail['propertyUrl'])
            external_id = str(property_detail['id']) 
            address = property_detail['displayAddress'] 
            city = ""
            zipcode = ""
            if len(address.split(",")) == 3:
                city = address.split(",")[-2].strip()
            elif len(address.split(",")) == 4:
                city = address.split(",")[-2].strip()
            elif  len(address.split(",")) == 2:
                city = address.split(",")[-2].strip()
            elif  len(address.split(",")) == 5:
                city = address.split(",")[-2].strip()         
            else: 
                city = address
            
           
            bathroom_count = property_detail["bathrooms"]
            room_count = property_detail['bedrooms']  
            lat = property_detail['location']['latitude']  
            lon = property_detail['location']['longitude'] 
            rent_string = property_detail['price']['displayPrices'][0]['displayPrice'] 
            images = []
            for img in property_detail['propertyImages']['images']:
                img_url = img['srcUrl']
                images.append(img_url)
            square_meters_text = property_detail['displaySize']
            description = property_detail['summary']
            item_loader = ListingLoader(response=response)
            item_loader.add_value('title', address)
            if square_meters_text:
                meters = square_meters_text.replace("sq. ft.","").replace(",","")
                s = str(int(float(meters) * 0.09290304))
                item_loader.add_value("square_meters", s)
            
            item_loader.add_value('external_link', external_link)            
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('description', description)
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_value('images', images)
            item_loader.add_value('furnished', True)
            if room_count:
                item_loader.add_value('room_count', str(room_count))
            if bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count)
            item_loader.add_value('latitude', str(lat))
            item_loader.add_value('longitude', str(lon))
            item_loader.add_value("external_source", self.external_source)

            yield scrapy.Request(external_link, callback=self.fix_issues, dont_filter=True, meta={'item_loader': item_loader,"property_type":response.meta.get('property_type')})        
    
    def fix_issues(self, response):
        item_loader = response.meta["item_loader"]

        bathroom_count = response.xpath("//div[contains(text(),'BATHROOMS')]/parent::div/following-sibling::div/div[2]/div/text()").get()
        if bathroom_count: 
            bathroom_count=bathroom_count.replace("x","")
            item_loader.add_value("bathroom_count",bathroom_count)
            
            

        import dateparser
        available_date = response.xpath("//dt[contains(.,'available date')]/following-sibling::dd/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['en'])
            if date_parsed: item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        city_zipcode = response.xpath("//h1/text()").get()
        if city_zipcode:
            zipcode = city_zipcode.split(',')[-1].strip().replace(".","")
            if " " in zipcode and zipcode.split(" ")[0].isalpha():
                zipcode = zipcode.split(" ")[-1]
                if zipcode.isalpha(): zipcode = ""
            else:
                if " " not in zipcode and zipcode.isalpha(): zipcode = ""
                elif zipcode.count(" ")>1 and not zipcode.split(" ")[-1].isalpha(): zipcode = zipcode.split(" ")[-1]
                elif zipcode.count(" ")>1 and zipcode.split(" ")[-1].isalpha(): zipcode = ""
            
            if zipcode:
                item_loader.add_value("zipcode", city_zipcode.split(',')[-1].strip().split(' ')[-1])
                

        
            # item_loader.add_value("city", item_loader.get_collected_values("address")[0].split(',')[-2].strip())

        studio = response.xpath("//div[@class='_2Pr4092dZUG6t1_MyGPRoL']/div[.='Studio']/text()").extract_first()
        if studio:
            item_loader.add_value('property_type', "studio")
            item_loader.add_value('room_count', "1")
        else:
            item_loader.add_value('property_type', response.meta.get('property_type'))
        
        deposit = "".join(response.xpath("//dt[contains(.,'Deposit')]/following-sibling::dd/text()").getall())
        if deposit: item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.split('.')[0])))

        parking = response.xpath("//div[h2[.='Property description']]//div/text()[contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        landlord_name = response.xpath("//div[@class='RPNfwwZBarvBLs58-mdN8']/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)

        landlord_phone = response.xpath("//a[@class='_3E1fAHUmQ27HFUFIBdrW0u']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)

        yield item_loader.load_item()