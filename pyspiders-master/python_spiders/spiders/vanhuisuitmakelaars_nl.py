# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from python_spiders.loaders import ListingLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
import dateparser
# from crawler.base import BaseSpider
import re

class MySpider(Spider):
    name = "vanhuisuitmakelaars_nl"
    start_urls = ["https://vanhuisuit-makelaars.nl/nl/aanbod/alle/huurwoningen-amsterdam"] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for follow_url in response.xpath("//div[contains(@class,'content-split')]//@href").extract():
            yield response.follow(follow_url, self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Vanhuisuitmakelaars_PySpider_" + self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//title/text()")

        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//article[@class='offer-detail-content']//text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        if desc:
            item_loader.add_value("description", desc)
        
        if " etage" in desc:
            floor=desc.split(" etage")[0].strip().split(" ")[-1]
            floor=floor_trans(floor)
            if floor:
                item_loader.add_value("floor",floor.strip())
        
        if "zwembad" in desc.lower() or "swimming pool" in desc.lower():
            item_loader.add_value("swimming_pool", True)
        
        if "no pets" in desc.lower():
            item_loader.add_value("pets_allowed", True)
        
        address= "".join(response.xpath("//h1//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//h1//span//text()").get()
        if city:
            item_loader.add_value("city", city)

        latLng = response.xpath("normalize-space(//script[contains(., 'lat')]/text())").get()
        if latLng:
            latitude,longitude = ScriptToLatLng(latLng)
            if latitude and longitude:
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
            

        property_type = response.xpath("//span[.='Type woning']/parent::*/span[2]/text()").get()
        if property_type and "Appartement" in property_type:
            item_loader.add_value("property_type", "apartment")
        elif property_type and "Woonhuis" in property_type:
            item_loader.add_value("property_type", "house")
        else:
            return
        

        square_meters = response.xpath("//span[.='Woonoppervlakte']/parent::*/span[2]/text()").get() 
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m2")[0].strip())    

        room_count = response.xpath("//span[.='Slaapkamer(s)']/parent::*/span[2]/text()").get()
        if room_count and room_count != "" and room_count != "-":
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom=response.xpath("//span[contains(.,'Badkamer')]/parent::*/span[2]/text()").get()
        if bathroom:
            bathroom=bathroom.split(" ")[0]
            if bathroom!="0":
                item_loader.add_value("bathroom_count", bathroom)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='detail-page-slider']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        price = response.xpath("//div[@class='price_bar']/div/h3/text()").get()
        if price and price != "-":
            if price.split(",")[0] and price.split(",")[0] not in ["-",""," "]:
                price = price.split(",")[0].strip("€").strip()          
                item_loader.add_value("rent", price)
        else:
            rent = "".join(response.xpath("substring-before(//div[@class='offer-detail-container']/aside//h3/text(),',')").extract())
            if rent:
                price = rent.replace(".","").replace(" ","").strip()
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        

        furnished = response.xpath("//span[.='Gemeubileerd']/parent::*/span[2]/text()").get()
        if furnished:
            if furnished.strip() == "Ja":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        landlord_name = response.xpath("//div[@class='last-section clearfix']/div/h3//text()").getall()
        if landlord_name and len(landlord_name) > 0:
            name = ""
            for x in landlord_name:
                name += x
            
        
        available_date=response.xpath("//div[@class='offer-detail-sticky-bar__content']/p[contains(.,'Beschikbaarheid')]/span/time/text()").get()

        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        item_loader.add_value("landlord_email", "info@vanhuisuit-makelaars.nl")
        item_loader.add_value("landlord_phone", "020 890 30 99")
        item_loader.add_value("landlord_name", "Vanhuisuit Makelaars")
        
        status = response.xpath("//div[contains(@class,'status-label')][not(contains(.,'Verhuurd'))]/text()").get()
        if status:
            yield item_loader.load_item()
        

def split_address(address, get):
    zip_code = "".join(filter(lambda i: i.isdigit(), address.split(",")[-2]))
    
    return zip_code
def ScriptToLatLng(latlng):
    latLngString = (latlng.split("google.maps.LatLng")[1]).split(";")[0].strip("('").strip("')")
    latitude = latLngString.split(",")[0].strip("'")
    longitude = latLngString.split(",")[1].strip().strip("'")
    return latitude,longitude

def floor_trans(floor):
    
    if floor.replace("e","").replace("ste","").isdigit():
        return floor.replace("e","")
    elif "eerste" in floor.lower():
        return "1"
    elif "tweede" in floor.lower():
        return "2"
    elif "derde" in floor.lower():
        return "3"
    elif "vierde" in floor.lower():
        return "4"
    elif "vijfde" in floor.lower():
        return "5"
    elif "achtste" in floor.lower():
        return "8"
    elif "bovenste" in floor.lower() or "hoogste" in floor.lower():
        return "upper"
    else :
        return False

