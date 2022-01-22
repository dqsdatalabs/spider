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
from datetime import datetime

class MySpider(Spider):
    name = 'hbe_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    external_source="Hbe_Co_PySpider_united_kingdom_en"
    start_urls = ["http://www.hbe.co.uk/properties-to-let?start=0"]

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 10)

        seen = False
        for item in response.xpath("//div[@id='smallProps']//a[@class='readmoreBtn']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            seen = True
        
        if page == 10 or seen:
            f_url = f'http://www.hbe.co.uk/properties-to-let?start={page}'
            yield Request(
                url=f_url,
                callback=self.parse,
                meta={"page" : page+10}
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        # let_src = response.xpath("//div[@class='eapow-bannertopright']/img/@alt[.='Let STC']").extract_first()
        # if let_src:
        #     return

        item_loader.add_value("external_link", response.url)   
        prop_type = response.xpath("//div[@class='control-label' and contains(.,'Price')]/text()").get()
        if prop_type and ("flat" in prop_type.lower() or "apartment" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("(//h1[@class='span8 pull-left']/text())[1]").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        rent=response.xpath("//h1/small[contains(.,'Monthly') or contains(.,'Price') or contains(.,'£')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",",""))
        
        room_count=response.xpath(
            "//div[@class='span12']/i[contains(@class,'bed')]/following-sibling::strong[1]/text()"
            ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count=response.xpath(
            "//div[@class='span12']/i[contains(@class,'bath')]/following-sibling::strong[1]/text()"
            ).get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        latitude_longitude=response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat: "')[1].split('"')[0]
            longitude = latitude_longitude.split('lon: "')[1].split('"')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        address = "".join(response.xpath("//h1/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(" ")[-1])
        
        zipcode = response.xpath("//address/text()").get()
        if zipcode:
            zipcode = zipcode.strip().split(" ")
            item_loader.add_value("zipcode", zipcode[-2] + " " + zipcode[-1])
            
        
        desc=" ".join(response.xpath("//div[contains(@class,'desc')]//p//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "unfurnished" in desc.lower():
            item_loader.add_value("furnished", False)
        elif " furnished" in desc.lower():
            item_loader.add_value("furnished", True)
        
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        
        if "dishwasher" in desc.lower():
            item_loader.add_value("dishwasher", True)
        
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            if "carpets" not in floor and "hardwood" not in floor and "and" not in floor :
                item_loader.add_value("floor", floor.replace("(","").replace(")",""))
        
        if "EPC rating of" in desc:
            energy = desc.split("EPC rating of")[1].strip().split(" ")[0]
        elif "EPC RATING" in desc:
            energy = desc.split("EPC RATING")[1].strip().split(" ")[0]
        elif "EPC" in desc:
            try:
                energy = desc.split("EPC")[1].split("=")[1].strip().split(" ")[0].split()[0]
                if "." in energy:
                    item_loader.add_value("energy_label", energy.split(".")[0])
                elif "'';" not in energy:
                    item_loader.add_value("energy_label", energy)
            except IndexError:
                pass
        
        if "deposit of" in desc:
            deposit = desc.split("deposit of")[1].strip().split(" ")[0].replace("£","")
            item_loader.add_value("deposit", deposit.split(".")[0])
        if "Rent of less than" in desc:
            deposit = desc.split("Rent of less than")[1].split(",")[0].split("\u00a3")[1].strip()
            item_loader.add_value("deposit", deposit)
        
        
        if "available now" in desc.lower():
            available_date = datetime.now()
            item_loader.add_value("available_date", available_date.strftime("%Y-%m-%d"))
        elif "Available un-furnished" in desc:
            available_date = desc.split("Available un-furnished from")[1].strip()
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        elif "Available late" in desc:
            available_date = desc.split("Available late")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        elif "AVAILABLE" in desc:
            available_date = desc.split("AVAILABLE")[1].split("Tenancy")[0].strip()
            try:
                date_parsed = dateparser.parse(
                        available_date, date_formats=["%m/%Y"]
                    )
                date2 = date_parsed.strftime("%Y-%m")
                item_loader.add_value("available_date", date2)
            except: pass
        elif "Available from mid" in desc:
            available_date = desc.split("Available from mid")[1].split("Tenancy")[0].strip()
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m")
            item_loader.add_value("available_date", date2)
        
        external_id=response.xpath("//div[@id='DetailsBox']/div[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        item_loader.add_value("landlord_name", "Hensons Estate Agents")
            
        item_loader.add_value("landlord_phone", "01275 810030")
        item_loader.add_value("landlord_email", "info@hbe.co.uk")
        
        images=[x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        floor_plan_images = response.xpath("//a/@title[contains(.,'Floor plan')]/parent::a/img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        yield item_loader.load_item()