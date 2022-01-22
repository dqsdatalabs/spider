# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'district-immo_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Districtimmo_PySpider_france'
    thousand_separator = ','
    scale_separator = '.' 
    
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "apartment"
            },
            {
                "property_type" : "house",
                "type" : "house"
            },
        ]
        for item in start_urls:
            formdata = {
                "action": "wpestate_custom_adv_ajax_filter_listings_search",
                "val_holder[]": "",
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "newpage": "1",
                "postid": "23708",
                "slider_min": "",
                "slider_max": "",
                "halfmap": "0",
                "all_checkers": "",
                "filter_search_action10": "",
                "adv_location10": "",
                "filter_search_action11": "louer",
                "filter_search_categ11": "appartement",
                "geo_lat": "",
                "geo_long": "",
                "geo_rad": "",
                "order": "3",
                "min_surface": "",
                "max_surface": "",
                "filter_search_secteur": "",
                "security": "79128da630",
            }
            yield FormRequest(
                "https://www.district-immo.com/wp-admin/admin-ajax.php",
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)["cards"]
        sel = Selector(text=data, type="html")

        for item in sel.xpath("//div[@class='item active']/a/@href").getall():
            follow_url = item
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            p_type = response.meta["type"]
            formdata = {
                "action": "wpestate_custom_adv_ajax_filter_listings_search",
                "val_holder[]": "",
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "val_holder[]": "", 
                "newpage": str(page),
                "postid": "23708",
                "slider_min": "",
                "slider_max": "",
                "halfmap": "0",
                "all_checkers": "",
                "filter_search_action10": "",
                "adv_location10": "",
                "filter_search_action11": "louer",
                "filter_search_categ11": "apartment",
                "geo_lat": "",
                "geo_long": "",
                "geo_rad": "",
                "order": "3",
                "min_surface": "",
                "max_surface": "",
                "filter_search_secteur": "",
                "security": "79128da630",
            }
            url = "https://www.district-immo.com/wp-admin/admin-ajax.php"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+1,
                    "type":p_type,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        title = response.xpath("//h1[@class='entry-title begum']/text()").get()
        if title:
            item_loader.add_value("title", title)  
        
        external_id = response.xpath("//span/text()[contains(.,'Réf')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(": ")[1]) 
    
        rent = response.xpath("//span[@class='h1 text-nowrap begum']/text()").get()
        if rent:
             item_loader.add_value("rent", rent.replace("€", "").strip().replace(" ", ""))  
        item_loader.add_value("currency", "EUR")
        city = response.url
        if city and "appartement-" in city:
            city = city.split('appartement-')[1].split('-')[1].split('-')[0]
            item_loader.add_value("city", city)
            if city:
                item_loader.add_value("address", city)        

        room_count =response.xpath("//div[@id='bien_specs']//span/text()[contains(.,'Pièce')]").get()    
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count =response.xpath("//div[@id='bien_specs']//span/text()[contains(.,'Chambre')]").get()    
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])        
   
        square =response.xpath("//div[@id='bien_specs']//span/sup/parent::span/text()").get()    
        if square:     
            item_loader.add_value("square_meters", square.split(" ")[0])

        deposit =response.xpath("//span[contains(.,'garantie :')]/parent::div/text()").get()    
        if deposit:     
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ", ""))
        
        utilities =response.xpath("//span[contains(.,'Charges :')]/parent::div/text()").get()    
        if utilities:     
            item_loader.add_value("utilities", utilities.split("€")[0])

        elevator = response.xpath("//span[contains(.,'Ascenseur :')]/parent::div/text()").get()
        if elevator and "non" not in elevator.lower():
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//span[contains(.,'Parking :')]/parent::div/text()").get()
        if parking and "non" not in parking.lower():
            item_loader.add_value("parking", True)
        
        images = [x for x in response.xpath("//img[@class='thumbnai_image']/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        latlng =response.xpath("(//script[@type='text/javascript']/text()[contains(.,'var mainLat=')])[1]").extract_first()    
        if latlng:
            item_loader.add_value("latitude",latlng.split("mainLat='")[1].split("'")[0].strip())
            item_loader.add_value("longitude",latlng.split("mainLon='")[1].split("'")[0].strip())
    
        desc = " ".join(response.xpath("//div[@class='gotham']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        
        item_loader.add_value("landlord_name", "DISTRICT GEORGE V")

        landlord_phone =response.xpath("//span[contains(@class,'phone_number')]/text()").get()    
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)  
        
        yield item_loader.load_item()