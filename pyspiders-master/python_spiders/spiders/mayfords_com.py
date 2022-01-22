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

class MySpider(Spider):
    name = 'mayfords_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ["https://www.mayfords.com/search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='port-title-cont']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='fdLinks']/@href").get())
            rent = item.xpath(".//div[@class='featuredProPrice']/div/text()[normalize-space()]").get()
            room = item.xpath(".//div[@class='itemRooms']/span[i[@class='icon-bed']]/preceding-sibling::text()[1]").get()
            bath = item.xpath(".//div[@class='itemRooms']/span[i[@class='icon-bath']]/preceding-sibling::text()[1]").get()
            yield Request(follow_url, callback=self.populate_item,meta={'rent': rent,'room': room,'bath': bath,})

         
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        item_loader.add_value("external_source", "Mayfords_PySpider_united_kingdom")     
        item_loader.add_value("external_id", response.url.split("/")[-1])
        bath = response.meta.get('bath')
        if bath:
            item_loader.add_value("bathroom_count",bath )
            
        deposit = response.xpath("//p/strong[contains(.,'Deposit')]/following-sibling::text()").get()
        rent = response.meta.get('rent')
        if rent:
            if "PW" in rent:
                rent = rent.split('£')[-1].strip().split('PW')[0].strip().replace(',', '').replace('\xa0', '')
                rent = str(int(float(rent)) * 4)
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string",rent)
                
            if deposit:
                deposit = deposit.split(" ")[0]
                rent = rent.replace("\n","").lower().replace("pcm","").replace("£","").strip().replace(",","")
                deposit = int(deposit)*int(float(int(rent)/4))
                item_loader.add_value("deposit", deposit)
            
        room = response.meta.get('room')
        if room and room.strip() !="0":
            item_loader.add_value("room_count",room ) 
        elif "studio" in get_p_type_string(f_text):
            item_loader.add_value("room_count", "1" ) 

        title =response.xpath("//h1/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip())   
        address = response.xpath("//h1/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())   
            zipcode = address.split(",")[-1].strip()
            city = address.split(",")[-2].strip()
            if city:
                item_loader.add_value("city",city)   
            if zipcode:
                item_loader.add_value("zipcode",zipcode)   
   
        balcony = response.xpath("//div[@id='detailsFeatures-tab']//li[contains(.,'Balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
      
        furnished = response.xpath("//div[@id='detailsFeatures-tab']//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        floor = response.xpath("//div[@id='detailsFeatures-tab']//li[contains(.,' floor') and not(contains(.,'flooring'))]//text()").extract_first()
        if floor:
            floor = floor.split("floor")[0].strip()
            item_loader.add_value("floor",floor.strip())     
     
        parking = response.xpath("//div[@id='detailsFeatures-tab']//li[contains(.,'Parking') or contains(.,'Garage')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)                
        desc = " ".join(response.xpath("//div[@class='fullDescription']/div[@class='descriptionsColumn']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip().replace("\t",""))
        available_date = " ".join(response.xpath("//div[@id='detailsFeatures-tab']//li[contains(.,'Available')]//text()").extract())
        if available_date:  
            date_parsed = dateparser.parse(available_date.split('Available')[1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-photos-device1']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div/a[i[@class='i-floorplan']]/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_value("landlord_name", "Mayfords")
        item_loader.add_value("landlord_phone", "020 8863 9718")
        item_loader.add_value("landlord_email", "office@mayfords.com")
        
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None