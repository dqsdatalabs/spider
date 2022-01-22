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
    name = 'londonstoneproperties_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    external_source = "Londonstoneproperties_PySpider_united_kingdom"
    start_urls = ["https://www.foxtons.co.uk/properties-to-rent/south-east-england/"]

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        
        seen = False
        for item in response.xpath("//div[@class='property_wrapper']/div/h6"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())       
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.londonstoneproperties.com/lettings/{page}/"
            yield Request(p_url, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//div[contains(text(),'Reference')]/following-sibling::div/text()").get()
        if externalid:
            item_loader.add_value("external_id",externalid)

        title = "".join(response.xpath("//title/text()").getall())
        desc = "".join(response.xpath("//div[@class='lc-d19-container']/p//text() | //p[@class='intro']//text()").getall())
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else:
                return

        if title:
            item_loader.add_value("title", title.strip())
        rent = " ".join(response.xpath("//a[contains(@class,'month')]/data/text()").extract())
        if rent:
            price = rent.replace("£","").strip().replace("\n", "").replace("\t", "").replace(",","").split(" ")[0]
            print(rent)
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        address = response.xpath("//div[@class='property-summary']/h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
        
        room_count = response.xpath("//img[contains(@alt,'bed')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//img[contains(@alt,'bath')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        meters = response.xpath("//dl[@id='terms']/dt[contains(.,'Total Sq Ft')]/following-sibling::dd[1]/text()").get()
        if meters:
            item_loader.add_value("square_meters", meters.split("(")[1].split(")")[0].strip().replace("Sq M","").strip())
        squ=item_loader.get_output_value("square_meters")
        if not squ:
            squ1=response.xpath("//div[contains(text(),'Total Sq Ft')]/following-sibling::div/text()").get()
            if squ1:
                item_loader.add_value("square_meters", squ1.split("(")[1].split(")")[0].strip().replace("Sq M","").strip())

        external_id = response.xpath("//dl[@id='terms']/dt[contains(.,'Reference')]/following-sibling::dd[1]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip() )
        
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))

        latitude_longitude = response.xpath("substring-after(//script/text()[contains(.,'latitude')],'latitude')").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(",")[0].replace('"','').replace(':','').strip()
            longitude = latitude_longitude.split('"longitude": ')[1].split("}")[0].replace('"','').strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        parking = response.xpath("//li[contains(.,'Parking')]/following-sibling::li/text()").get()
        if parking:
            if "No" in parking:
                item_loader.add_value("parking", False)
            elif "Yes" in parking:
                item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Outside Space')]/following-sibling::li/text()").get()
        if balcony:
            if "balcony" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]/following-sibling::li/text()").get()
        if furnished:
            if "un" in furnished:
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished:
                item_loader.add_value("furnished", True)
        
        item_loader.add_xpath("images", "//meta[@property='og:image']/@content")
        images = response.xpath("//script[contains(.,'var src = \"')]/text()").get()
        image = images.split('var src = "')
        for i in range(1,len(image)):
            item_loader.add_value("images", image[i].split('"')[0])
        
        floor_plan_images = response.xpath("//div[@id='floorplan_preview']/a/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", response.urljoin(floor_plan_images))
        
        item_loader.add_value("landlord_name", "LONDON STONE PROPERTIES")
        city=item_loader.get_output_value("city")
        if city=="Mayfair":
            city="Marylebone" 
        
        city=city.replace(" ","").replace("'","")
        item_loader.add_value("landlord_email",f"{city}@foxtons.co.uk")
        
        phone = response.xpath("//span[@class='telephone']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        else:
            item_loader.add_value("landlord_phone", "020 7433 6644")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
