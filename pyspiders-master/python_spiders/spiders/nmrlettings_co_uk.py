# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'nmrlettings_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Nmrlettings_PySpider_united_kingdom_en"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.nmrlettings.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.nmrlettings.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=3&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                    "https://www.nmrlettings.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1"
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                type_group = item.split("PropertyTypeGroup=")[1].split("&")[0].strip()
                yield Request(item,
                            callback=self.parse,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type'), "type_group":type_group})
        yield Request("https://www.nmrlettings.co.uk/properties.aspx?mode=1&rentalstudents=1&menuID=5",
                            callback=self.parse,
                            dont_filter=True,
                            meta={'property_type': "student_apartment"})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[contains(@class,'status-text') and not(contains(.,'Let'))]/preceding-sibling::a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if (page == 2 or seen) and response.meta.get("property_type") != "student_apartment":
            paging_info = response.xpath("//a[@class='chzn-single' and contains(.,'Page')]/span/text()").get()
            if paging_info:
                total_page = paging_info.split("of")[1].strip()
                type_group = response.meta.get("type_group")
                if type_group == "Student":
                    return
                
                view_state = response.xpath("//input[@name='__VIEWSTATE']/@value").get()
                view_state_gen = response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get()
                event_target = response.xpath("//input[@name='__EVENTTARGET']/@value").get()
                event_val = response.xpath("//input[@name='__EVENTVALIDATION']/@value").get()

                p_url = f"https://www.nmrlettings.co.uk/properties.aspx?Mode=1&PropertyTypeGroup={type_group}&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1"
                formdata = {
                    "__EVENTTARGET": event_target,
                    "__EVENTARGUMENT": "",
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": view_state,
                    "__VIEWSTATEGENERATOR": view_state_gen,
                    "__EVENTVALIDATION": event_val,
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$txtSearch": "",
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup": "",
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboBedrooms": "0",
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMinPrice": "0",
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMaxPrice": "0",
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboStatus": "Show All",
                    "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
                    "ctl00$ContentPlaceHolderMain$cboPageNos": f"Page {page} of {total_page}",
                }
                yield FormRequest(
                    url=p_url,
                    callback=self.parse,
                    formdata=formdata,
                    dont_filter=True,
                    meta={
                        "page":page+1,
                        "type_group":type_group,
                        "property_type":response.meta.get("property_type")
                    }
                )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)


        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//div//h1[@id='banner-title']//text()").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
            item_loader.add_value("address",title.strip()) 
            item_loader.add_value("city",title.split(", ")[-1].strip()) 
  
        
        rent = response.xpath("//div[contains(@id,'ContentPlaceHolderMain_divPrice')]/span[contains(.,'£')]/text()").extract_first()
        if rent:
            if "pw" in rent:
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent.replace(",","."))

  
            deposit = response.xpath("//span[@id='ctl00_ContentPlaceHolderMain_lblPropertyMainDescription']/text()[contains(.,'Deposit')]").extract_first()
            if deposit:
                if "£" in deposit:
                    c_deposit =  deposit.split("£")[1].split(" ")[0]
                    item_loader.add_value("deposit", c_deposit)
                elif "Months" in deposit:
                    m_deposit = deposit.split("Months")[0].strip().split(" ")[-1]

                    item_loader.add_value("deposit", int(m_deposit)*rent.replace(",","."))
            
        
        room = response.xpath("//ul[@class='amenities']//li[i[@class='icon-bedrooms']]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())
        bathroom_count = response.xpath("//ul[@class='amenities']//li[i[@class='icon-bathrooms']]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        square_meters = response.xpath("//ul[@class='amenities']//li[i[@class='icon-area']]/text()[contains(.,'m²')]").extract_first()
        if square_meters:
            square = square_meters.split("m²")[0]
            item_loader.add_value("square_meters", square.strip())
        furnished = response.xpath("//div//h2//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        parking = response.xpath("//ul[contains(@class,'property-features')]/li[contains(.,'Parking') or contains(.,'Garage')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//ul[@class='amenities']/li[contains(.,'Terrace')]//text()").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = "".join(response.xpath("//ul[@class='property-features col-sm-6']/li[contains(.,'Furnished')]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True)

     
        available_date = response.xpath("//div//h2//text()[contains(.,'Available')][not(contains(.,'Available Now'))]").extract_first()
        if available_date:  
            try:
                date_format = available_date.split("Available")[1].split(" -")[0].strip()
                newformat = dateparser.parse(date_format).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            except:
                pass
                
        map_coordinate = response.xpath("//div[@id='tabStreetView']//iframe/@src").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split('&cbll=')[1].split('&')[0]
            latitude = map_coordinate.split(',')[0].strip()
            longitude = map_coordinate.split(',')[1].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        desc = " ".join(response.xpath("//span[contains(@id,'lblPropertyMainDescription')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "No Smokers or Pets" in desc:
                item_loader.add_value("pets_allowed", False)
 
        images = [x for x in response.xpath("//div[@id='property-detail-thumbs']//div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0191 300 6112")
        item_loader.add_value("landlord_email", "info@nmrlettings.co.uk")
        item_loader.add_value("landlord_name", "NMR LETTINGS")

        yield item_loader.load_item()
