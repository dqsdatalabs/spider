# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
import dateparser

class MySpider(Spider):
    name = 'amberandco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Amberandco_PySpider_united_kingdom_en"
    
    def start_requests(self):

        yield Request(
            url="https://www.amberandco.co.uk/properties.aspx?Mode=1&PriceMax=0&Bedrooms=0&Areas=",
            callback=self.parse,
            dont_filter=True,
        )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)
        seen = False

        for item in response.xpath("//div[contains(@class,'item col-md-4')]//div[@class='info']/h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        view_state=" ".join(response.xpath("//input[@id = '__VIEWSTATE']/@value").extract())
        event_validation=" ".join(response.xpath("//input[@id = '__EVENTVALIDATION']/@value").extract())
    
        if page == 2 or seen:
            formdata = {
                "_EVENTTARGET": "ctl00$ContentPlaceHolderMain$repPages$ctl02$lnkPage",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": f"{view_state}",
                "__VIEWSTATEGENERATOR": "050B8BD0",
                "__EVENTVALIDATION": f"{event_validation}",
                "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
                "ctl00$ContentPlaceHolderMain$cboPageNos": f"Page {page} of 6",
            }
            yield FormRequest(
                url="https://www.amberandco.co.uk/Properties.aspx?mode=1&menuID=41",
                formdata=formdata,
                dont_filter=True,
                callback=self.parse,
                meta={"page": page+1}
                
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = ""
        p_type = "".join(response.xpath("//span[contains(@id,'PropertyType')]/text()").get())
        if p_type and ("Flat" in p_type or "Terrace" in p_type or "Apartment" in p_type):
            prop_type = "apartment"
        elif p_type and ("House" in p_type or "Penthouse" in p_type) :
           prop_type = "house"
        elif p_type and ("Room" in p_type or "Studio" in p_type) :
           prop_type = "studio"
        else:
           return

        item_loader.add_value("property_type", prop_type)

        item_loader.add_value("external_source", "Amberandco_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@class='main col-sm-12']/h2//text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            if "unfurnished" in title.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in title.lower():
                item_loader.add_value("furnished", True)

        address = response.xpath("//title/text()").get()
        if address:
            item_loader.add_value("address", address.split(" - ")[1].strip())
            item_loader.add_value("external_id", address.split("ID")[1].split(")")[0].strip())
            address = address.split(" - ")[1].strip()

        square_meters=response.xpath("//ul[@class='amenities']/li[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        

        room_count = response.xpath("//i[@class='icon-bedrooms']//parent::li//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split())
        if prop_type=="studio":
            item_loader.add_value("room_count", 1)
        bathroom = response.xpath("//i[@class='icon-bathrooms']//parent::li//text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split())

        rent= response.xpath("//div[@class='main col-sm-12']/h2//text()").get()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split('£')[-1].lower().split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent)

        images = [x for x in response.xpath("//div[@id='property-detail-large']/div//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))   

        floor_plan_images = response.xpath("//div[@id='tabFloorPlan']//img//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "Amber&Co")
        item_loader.add_value("landlord_phone", "0208 740 9944")
        item_loader.add_value("landlord_email", "info@amberandco.co.uk")
        
        script_map = response.xpath("//div[@id='tabStreetView']/iframe/@src").get()
        if script_map:
            latlng = script_map.split("&cbll=")[1].split("&")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        yield item_loader.load_item()

