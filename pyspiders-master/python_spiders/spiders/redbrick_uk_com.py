# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime 
import dateparser

class MySpider(Spider):  
    name = 'redbrick_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.redbrick.uk.com/"] 
    external_source='Redbrickproperties_PySpider_united_kingdom_en'
    # custom_settings = {
    #     "HTTPCACHE_ENABLED": False,
    # }  

    def parse(self, response):
        for item in response.xpath("//li[@id='menu-item-833']/ul/li/a/@href").getall():
            yield Request(
                item,
                callback=self.goto_on_the_market,
            )
    
    def goto_on_the_market(self, response):
        site_link = response.xpath("//div[@class='ugb-button-container']/a/@href").get()
        if site_link:
            yield Request(
                site_link,
                callback=self.jump,
                meta={"first":True}
            )

    # 1. FOLLOWING
    def jump(self, response):

        if response.meta.get("first", False):
            redirect_url_house = response.url.replace("property", "houses")
            yield Request(
                redirect_url_house,
                callback=self.jump,
                meta={"p_type":"house"},
            )
            redirect_url_apt = response.url.replace("property", "flats-apartments")
            yield Request(
                redirect_url_apt,
                callback=self.jump,
                meta={"p_type":"apartment"},
            )
        else:
            for item in response.xpath("//a[@itemprop='photo']"):
                follow_url = response.urljoin(item.xpath("./@href").get())
                yield Request(follow_url, callback=self.populate_item, meta={"p_type":response.meta.get("p_type")})
            
            next_page = response.xpath("//a[@title='Next page']/@href").get()
            if next_page:
                yield Request(response.urljoin(next_page), callback=self.jump, meta={"p_type":response.meta.get("p_type")})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("p_type"))
        rent = response.xpath("//span[@class='price-data']/text()").get()
        if rent:
            price = rent.split("pcm")[0].split("£")[1].replace(",","").strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency","GBP")
        dontallow=response.xpath("//h1[@class='retracted__title']/text()").get()
        if dontallow and "This property" in dontallow:
            return 

        zipcode_externalID = response.xpath("//script[contains(.,'postcode') and contains(.,'property-id')]/text()").get()
        if zipcode_externalID:
            zipcode = zipcode_externalID.split('postcode":"')[1].split('"')[0]
            item_loader.add_value("zipcode", zipcode)
            external_id = zipcode_externalID.split('property-id":')[1].split(',')[0]
            item_loader.add_value("external_id", external_id)
                    
        script_page = response.xpath("//script[contains(.,'__OTM__.jsonData =')]//text()").get()
        print(script_page)
        if script_page:
            json_data = script_page.split("__OTM__.jsonData = ")[-1].split("};")[0].strip()+"}"
            item = json.loads(json_data)
            if "bedrooms" in item:
                if str(item["bedrooms"]) !='0':
                    item_loader.add_value("room_count", str(item["bedrooms"])) 
            if "bathrooms" in item:
                if str(item["bathrooms"]) !='0':
                    item_loader.add_value("bathroom_count", str(item["bathrooms"])) 
            item_loader.add_value("title", item["property-title"]) 
            item_loader.add_value("address", item["display_address"]) 
            city = item["address-locality"].split(",")[-1]
            item_loader.add_value("city", city.strip()) 
            if "location" in item:
                item_loader.add_value("longitude", str(item["location"]["lon"]))
                item_loader.add_value("latitude", str(item["location"]["lat"]))

            item_loader.add_value("description", item["description"]) 
            parking = response.xpath("//li[contains(.,'parking')]//text()").get()
            if parking:
                item_loader.add_value("parking", True)

            if "let-info" in item:
                if "furnished-text" in item["let-info"]:
                    furnished = item["let-info"]["furnished-text"]
                    if "unfurnished" in furnished.lower():
                        item_loader.add_value("furnished", False) 
                    elif "furnished" in furnished.lower():
                        item_loader.add_value("furnished", True) 
                if "let-date-available" in item["let-info"] and "let-date-available-now?" in item["let-info"]:
                    if item["let-info"]["let-date-available-now?"]:
                        item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                    else:
                        date_parsed = dateparser.parse(item["let-info"]["let-date-available"], date_formats=["%d/%m/%Y"])
                        if date_parsed:
                            date2 = date_parsed.strftime("%Y-%m-%d")
                            item_loader.add_value("available_date", date2)
                            
                if "deposit-amount" in item["let-info"]:
                    item_loader.add_value("deposit", item["let-info"]["deposit-amount"])
            if "agent" in item:
                item_loader.add_value("landlord_name", item["agent"]["name"])
                item_loader.add_value("landlord_phone", item["agent"]["telephone"])
                landlord_email = item["agent"]["lettings"]["email"]
                if landlord_email:
                    item_loader.add_value("landlord_email", landlord_email)
                else:
                    item_loader.add_value("landlord_email", "lettings@neilsutherland.co.uk")

            images = [x["large-url"] for x in item["images"]]
            if images:
                item_loader.add_value("images", images)
            if item["floorplans?"]:
                floor_plan_images = [x["large-url"] for x in item["floorplans"]]
                if floor_plan_images:
                    item_loader.add_value("floor_plan_images", floor_plan_images)
            try:
                if item["letting-details"]:
                    if item["letting-details"]["items"]:
                        available_date=str(item["letting-details"]["items"]).split("date:")[-1].split("Unfur")[0].split("',")[0].strip()
                        if available_date:
                            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                            if date_parsed:
                                date2 = date_parsed.strftime("%Y-%m-%d")
                                item_loader.add_value("available_date", date2)
                        if "Furnished" in str(item["letting-details"]["items"]):
                            item_loader.add_value("furnished",True)
                            
            except:
                pass
            try:
                if "Deposit" in str(item["letting-details"]["items"]):
                    deposit=str(item["letting-details"]["items"]).split("Deposit")[-1].split(":")[-1].split(".")[0].replace("£","")
                    item_loader.add_value("deposit",deposit)
            except:
                pass
        else:
            title = response.xpath("//div[@class='details-heading']/h1/text()").get()
            if title:
                item_loader.add_value("title", title.strip())
                if "studio" in title.lower():
                    item_loader.add_value("room_count","1")
                elif "bedroom" in title:
                    room_count = title.split("bedroom")[0].strip()
                    item_loader.add_value("room_count", room_count)
                
            address = response.xpath("//div[@class='details-heading']/*[not(self::p[@class='price']) and self::p]/text()").get()
            if address:
                
                address = address.strip()
                item_loader.add_value("address", address)
                city = ""
                if "-" in address:
                    city = address.split("-")[0].strip()
                elif "BN" in address:
                    city = address.split("BN")[0].strip().strip(",").split(",")[-1].strip()
                else:
                    if "," in address:
                        city = address.split(",")[-1].strip()
                    else:
                        city = address.strip().split(" ")[-1]
                
                cities = ["brighton", "sussex", "hove", " road", " street", "kingsway","hollingbury","moulscombe","bevendean"]
                for i in cities:
                    if i in city.lower():
                        item_loader.add_value("city", city)

            desc = "".join(response.xpath("//div[@id='description-text']//text()").getall())
            if desc:
                item_loader.add_value("description", desc.strip())
            
            desc = desc.replace(".","").replace("\u00a0"," ")
            if "square meters" in desc:
                square_meters = desc.split("square meters")[0].strip().split(" ")[-1]
                if "/" in square_meters:
                    square_meters = square_meters.split("/")[-1]
                item_loader.add_value("square_meters", square_meters)
            elif "sq m" in desc:
                square_meters = desc.split("sq m")[0].strip().split(" ")[-1].replace("(","")
            elif "sq ft" in desc:
                sq_f = desc.split("sq ft")[0].strip().split(" ")[-1]
                sqm = str(int(int(sq_f)* 0.09290304))
                item_loader.add_value("square_meters", sqm)
            elif " ft" in desc:
                sq_f = desc.split(" ft")[0].strip().split(" ")[-1]
                if sq_f.isdigit():
                    sqm = str(int(int(sq_f)* 0.09290304))
                    item_loader.add_value("square_meters", sqm)
                    
            available_date = response.xpath("//div[@class='letting-details']//li[contains(.,'Availab') and not(contains(.,'furnished'))]/text()").get()
            if available_date:
                if "now" in available_date:
                    available_date = datetime.now().strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", available_date)
                else:
                    
                    available_date = available_date.split(":")[1].strip()
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)


            energy_label = response.xpath("//div[@id='description-text']//text()[contains(.,'EPC Rating') or contains(.,'EPC RATING')]").get()
            if energy_label:
                energy_label = energy_label.split(" ")[-1].replace("(","").replace(")","").replace(".","")
                if energy_label and energy_label.isdigit():
                    item_loader.add_value("energy_label", energy_label_calculate(energy_label))
                else:
                    try:
                        item_loader.add_value("energy_label", energy_label[0])
                    except:
                        item_loader.add_value("energy_label", energy_label)
                        
            images = [x for x in response.xpath("//noscript//img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
            
            floor_plan_images = response.xpath("//noscript//div[contains(.,'Floorplan')]/a/@href").get()
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)
            
            latitude_longitude = response.xpath("//div[@id='details-map']/noscript//img/@src").get()
            if latitude_longitude:
                latitude = latitude_longitude.split("center=")[1].split(",")[0]
                longitude = latitude_longitude.split("center=")[1].split(",")[1].split("&")[0]
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
                
            deposit = response.xpath("//div[@class='letting-details']//li[contains(.,'Deposit')]/text()").get()
            if deposit:
                deposit = deposit.split("£")[1].strip().replace(" ","")
                item_loader.add_value("deposit", deposit)
            
            unfurnished = response.xpath("//div[@class='letting-details']//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]/text()").get()
            furnished = response.xpath("//div[@class='letting-details']//li[contains(.,'furnished') or contains(.,'Furnished')]/text()").get()
            if unfurnished:
                item_loader.add_value("furnished", False)
            elif furnished:
                item_loader.add_value("furnished", True)
            
            parking = response.xpath("//div[@id='description-text']//text()[contains(.,'Parking')]").get()
            parking2 = response.xpath("//li[contains(.,'parking')]//text()").get()
            if parking or parking2:
                if parking and "permit" in parking:
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", True)
            
            landlord_name = response.xpath("//h2[@class='agent-name']/text()").get()
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name.strip())
            
            landlord_phone = response.xpath("//div[@class='agent-phone-link']/a/text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
            item_loader.add_value("landlord_email", "lettings@neilsutherland.co.uk")
                    
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label