# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'stonebridgelondon_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "ASP.NET_SessionId=ymtaxdy5bqbvhss422yucdth; _ga=GA1.3.904919626.1616137568; _gid=GA1.3.133520585.1616137568",
        "Origin": "https://www.stonebridgelondon.co.uk",
        "Referer": "https://www.stonebridgelondon.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&Statusid=0&searchstatus=1&ShowSearch=1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
    }

    def start_requests(self):
        yield Request("https://www.stonebridgelondon.co.uk/properties.aspx?Mode=1", callback=self.jump)
    
    def jump(self, response):
        view_state = response.xpath("//input[@name='__VIEWSTATE']/@value").get()
        view_state_gen = response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get()
        event_val = response.xpath("//input[@name='__EVENTVALIDATION']/@value").get()

        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "Flats/Apartments"
            },
            {
                "property_type" : "house",
                "type" : "Houses"
            },
        ]
        for item in start_urls:
            formdata = {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": view_state_gen,
                "__EVENTVALIDATION": event_val,
                "ctl00$ContentPlaceHolderMain$PropertySearch$txtSearch": "",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboPropertyTypeGroup": item["type"],
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboCategory": "For Rent",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboBedrooms": "0",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboMinPrice": "0",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboMaxPrice": "0",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboStatus": "Just Available",
                "ctl00$ContentPlaceHolderMain$PropertySearch$butSearch": "Search",
            }
            yield FormRequest(
                "https://www.stonebridgelondon.co.uk/properties.aspx?Mode=1",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                headers=self.headers,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'item')]//h3/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            view_state = response.xpath("//input[@name='__VIEWSTATE']/@value").get()
            view_state_gen = response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get()
            event_val = response.xpath("//input[@name='__EVENTVALIDATION']/@value").get()
            p_type = response.meta["type"]
            if p_type == "Houses":
                p_type_group = "1"
            else:
                p_type_group = "2"

            formdata = {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": view_state_gen,
                "__EVENTVALIDATION": event_val,
                "ctl00$ContentPlaceHolderMain$PropertySearch$txtSearch": "",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboPropertyTypeGroup": p_type,
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboCategory": "For Rent",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboBedrooms": "0",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboMinPrice": "0",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboMaxPrice": "0",
                "ctl00$ContentPlaceHolderMain$PropertySearch$cboStatus": "Just Available",
                "ctl00$ContentPlaceHolderMain$lstSort": "Sort Newest Added",
                "ctl00$ContentPlaceHolderMain$cboPageNos": f"Page {page} of 1000",
            }
            yield FormRequest(
                f"https://www.stonebridgelondon.co.uk/properties.aspx?Mode=1&PropertyTypeGroup={p_type_group}&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                headers=self.headers,
                meta={
                    "property_type":response.meta["property_type"],
                    "type":response.meta["type"],
                    "page":page+1
                })

         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Stonebridgelondon_Co_PySpider_united_kingdom")      

        item_loader.add_xpath("external_id", "//span[contains(@id,'lblPropertyID')]/text()")

        title =response.xpath("//h1[@id='banner-title']/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip())   
        address = response.xpath("//h1[@id='banner-title']/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())   
            zipcode = address.split(",")[-1].strip()
            zipcode = ' '.join(c for c in zipcode.split(' ') if not c.isalpha()).strip()
            city = ""
            if zipcode.isalpha():
                city = address.split(",")[-1].strip()
            else:
                city = " ".join(address.split(",")[-1].strip().split(" ")[:-1])
                if zipcode.count(" ") > 0 and city.isalpha():
                    pass 
                else:
                    city = address.split(",")[-2].strip()
            if city:
                item_loader.add_value("city",city)   
            if zipcode:
                item_loader.add_value("zipcode", zipcode)   
                           
        rent = " ".join(response.xpath("//div[contains(@class,'price')]/span/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent)

        square_meters = response.xpath("//div[@class='property-topinfo']/ul/li[i[@class='icon-area']]/text()[contains(.,'m')]").extract_first() 
        if square_meters:   
            square_meters = square_meters.strip().split("m")[0]
            item_loader.add_value("square_meters", square_meters)
        room_count = response.xpath("//div[@class='property-topinfo']/ul/li[i[@class='icon-bedrooms']]/text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count", room_count)
        bathroom = response.xpath("//div[@class='property-topinfo']/ul/li[i[@class='icon-bathrooms']]/text()").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count",bathroom) 
  
        terrace = response.xpath("//div[@class='property-topinfo']/ul/li[i[@class='icon-apartment']]//text()[contains(.,'Terrace')]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
      
        furnished = response.xpath("//div/h2//text()[contains(.,'furnished') or contains(.,'Furnished')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        floor = response.xpath("substring-before(//ul/li[contains(.,'FLOOR') and not(contains(.,'FLOORING'))]//text(),'FLOOR')").extract_first()
        if floor:
            if "BEDROOM" in floor:
                floor = floor.split("BEDROOM")[1].strip()
            item_loader.add_value("floor",floor.strip())     
        energy = response.xpath("substring-after(//ul/li[contains(.,'EPC')]//text(),'EPC')").extract_first()
        if energy:
            if energy.strip().isalpha():
                item_loader.add_value("energy_label",energy.strip())       
        parking = response.xpath("//ul/li[contains(.,'PARKING') or contains(.,'GARAGE')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)                
        desc = " ".join(response.xpath("//span[contains(@id,'lblPropertyMainDescription') ]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        available_date = " ".join(response.xpath("//ul/li[contains(.,'AVAILABLE') ]//text()").extract())
        if available_date:  
            if "IMMEDIATELY" in available_date.upper():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.split('AVAILABLE')[1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-detail-large']/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//div[@id='tabStreetView']/iframe/@src").get()
        if script_map:
            latlng = script_map.split("&cbll=")[1].split("&cbp=")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
 
        item_loader.add_value("landlord_name", "STONEBRIDGE LONDON LTD")
        item_loader.add_value("landlord_phone", "020 8590 1499")
        item_loader.add_value("landlord_email", "info@stonebridgelondon.co.uk")
        yield item_loader.load_item()