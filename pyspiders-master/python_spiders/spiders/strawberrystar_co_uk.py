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
    name = 'strawberrystar_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://strawberrystar.co.uk/property-search/?address_keyword=&department=residential-lettings']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='thumbnail']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://strawberrystar.co.uk/property-search/page/{page}/?address_keyword&department=residential-lettings"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[@class='description']//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        
        item_loader.add_value("external_source", "Strawberrystar_Co_PySpider_united_kingdom")  
        item_loader.add_xpath("title", "//h1/text()")
     
        balcony = response.xpath("//li/text()[contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)   
        swimming_pool = response.xpath("//li/text()[contains(.,'swimming pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)   
        parking = response.xpath("//li/text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 
     
        available_date = response.xpath("substring-after(//div/text()[contains(.,'Available From:')],':')").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())     
            city = address.split(",")[-1].strip()
            if "road" not in city.lower():
                item_loader.add_value("city", city)

        item_loader.add_xpath("room_count","substring-before(//li[@class='bedrooms']/text(),'Bed')")
        item_loader.add_xpath("bathroom_count","substring-before(//li[@class='bathrooms']/text(),'Bath')")
     
        rent = response.xpath("//div[@class='done price']/div/text()").get()
        if rent:
            rent = rent.split("£")[-1].split("p")[0].replace(",","").strip()
            item_loader.add_value("rent", str(int(rent)*4))
            item_loader.add_value("currency", "GBP")
     
        description = " ".join(response.xpath("//div[@class='description']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='slider']/ul[@class='slides']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x.split("(`")[1].split("`")[0] for x in response.xpath("//li[@class='action-floorplans']/a/@onclick").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
 
        item_loader.add_xpath("landlord_name", "substring-after(//div[@class='call']/text(),'Call ')")
        item_loader.add_xpath("landlord_phone", "//div[@class='call']/a/text()")
        lat_lng = response.xpath("//script[contains(.,' myLatlng = new google.maps.LatLng(')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(" myLatlng = new google.maps.LatLng(")[-1].split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(" myLatlng = new google.maps.LatLng(")[-1].split(",")[1].split(")")[0])
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return