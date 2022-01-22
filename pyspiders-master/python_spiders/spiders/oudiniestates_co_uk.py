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

class MySpider(Spider):
    name = 'oudiniestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {
                "flag" : "Flats/Apartments",
                "url" : [
                    "https://www.oudiniestates.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type" : "apartment"
            },
            {
                "flag" : "Houses",
                "url" : [
                    "https://www.oudiniestates.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&Statusid=0&searchstatus=1&ShowSearch=1",
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
                            meta={'property_type': url.get('property_type'), "type_group":type_group, "flag":url.get("flag")})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='image']//a[contains(@id,'repPropertyList')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            paging_info = response.xpath("//option[@selected='selected' and contains(@value,'Page')]/text()").get()
            if paging_info:
                total_page = paging_info.split("of")[1].strip()
                type_group = response.meta.get("type_group")
                flag = response.meta.get("flag")
            
                view_state = response.xpath("//input[@name='__VIEWSTATE']/@value").get()
                view_state_gen = response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get()
                event_val = response.xpath("//input[@name='__EVENTVALIDATION']/@value").get()

                p_url = f"https://www.oudiniestates.co.uk/properties.aspx?Mode=1&PropertyTypeGroup={type_group}&PriceMin=0&PriceMax=0&Bedrooms=0&Statusid=0&searchstatus=1&ShowSearch=1"
                formdata = {
                    "__EVENTTARGET": "ctl00$ContentPlaceHolderMain$lnkPageNext",
                    "__EVENTARGUMENT": "",
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": view_state,
                    "__VIEWSTATEGENERATOR": view_state_gen,
                    "__EVENTVALIDATION": event_val,
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$txtSearch": "",
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup": flag,
                    "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboCategory": "For Rent",
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
                        "property_type":response.meta.get("property_type"),
                        "flag":response.meta.get("flag"),
                    }
                )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Oudiniestates_Co_PySpider_united_kingdom")

        p_type = response.meta.get('property_type')
        p_type_check = response.xpath("//span[contains(@id,'PropertyType')]/text()").get()

        if p_type_check.lower().strip() in ["semi-detached"]:
            item_loader.add_value("property_type", "apartment")
        else:
            item_loader.add_value("property_type", p_type)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1].split(".")[0])
        desc = " ".join(response.xpath("//span[contains(@id,'lblPropertyMainDescription')]//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())
        else:
            desc = "" 
         
        sq = response.xpath("substring-before(//li/i[@class='icon-area']/following-sibling::text(),'m²')").extract_first()
        if sq:   
            item_loader.add_value("square_meters", int(float(sq.replace(",","."))))
        elif not sq and desc:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",desc.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        room_count = response.xpath("//li/i[@class='icon-bedrooms']/following-sibling::text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        bathroom_count = response.xpath("//li/i[@class='icon-bathrooms']/following-sibling::text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        address = response.xpath("//div/h1/text()").extract_first()
        if address:
            item_loader.add_value("title", address.strip())
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
        
        rent = response.xpath("//div/h2//text()[contains(.,'£')]").extract_first()
        if rent:    
            if "pw" in rent:
                rent = rent.split('£')[1].split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent)    
  
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-detail-large']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='tabFloorPlan']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        map_coordinate = response.xpath("//iframe/@src[contains(.,'maps')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split("&cbll=")[1].split("&")[0].strip()
            latitude = map_coordinate.split(",")[0].strip()
            longitude = map_coordinate.split(",")[1].strip()          
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
        
        balcony = response.xpath("//ul[contains(@class,'property-features')]/li//text()[contains(.,'Balcony') or contains(.,'balcony')]").extract_first()
        if balcony: 
            item_loader.add_value("balcony", True) 
    
        parking = response.xpath("//ul[contains(@class,'property-features')]/li//text()[contains(.,'Parking') or contains(.,'parking')]").extract_first()
        if parking: 
            item_loader.add_value("parking", True) 

        elevator = response.xpath("//ul[contains(@class,'property-features')]/li//text()[contains(.,'Lift')]").extract_first()
        if elevator: 
            item_loader.add_value("elevator", True) 
        pool = response.xpath("//ul[contains(@class,'property-features')]/li//text()[contains(.,' Pool')]").extract_first()
        if pool: 
            item_loader.add_value("swimming_pool", True) 

        furnished = response.xpath("//ul[contains(@class,'property-features')]/li//text()[contains(.,'furnished') or contains(.,'Furnished')]").extract_first()
        if furnished: 
            if "furnished or unfurnished" in furnished.lower():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)  
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True) 
        

        if not item_loader.get_collected_values("furnished") and "unfurnished" not in desc.lower() and "furnished" in desc.lower():
            item_loader.add_value("furnished", True)
        
        if not item_loader.get_collected_values("parking") and "parking" in desc.lower():
            item_loader.add_value("parking", True)
      
        item_loader.add_value("landlord_email", "info@oudiniestates.co.uk")
        item_loader.add_value("landlord_phone", "020 7112 8436")
        item_loader.add_value("landlord_name", "OUDINI ESTATES")
        
        yield item_loader.load_item()
