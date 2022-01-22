# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n
import re 
 
class MySpider(Spider):
    name = 'lauristons_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Lauristons_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.lauristons.com/properties-to-rent/all-properties",
                ],
            }
                

        ]  
        # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='main_image'][not(contains(.,'Let Agreed'))]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"/page/{page-1}", f"")+f"/page/{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("id-")[1])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        property_type="".join(response.xpath("//div[@id='property_description']/text() | //div[@class='lists']/following-sibling::p//text()").getall())
        if property_type:
                    
            item_loader.add_value("property_type", get_p_type_string(property_type))


        
        address = response.xpath("//h2/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            
        rent = response.xpath("//p[contains(@class,'salePrice')]/text()").get()
        if rent:
            rent = rent.replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        deposit = response.xpath("substring-after(//span[@class='deposit']/text(),'£')").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",""))
        
        room_count = response.xpath("//div[contains(@class,'beds')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//div[contains(@class,'bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except: pass
        bathcheck=item_loader.get_output_value("bathroom_count")
        if not bathcheck:
            bathroom=response.xpath("//div[@class='list-group']//ul//li//text()[contains(.,'Bath')]").get()
            if bathroom:
                bathroom= bathroom.split(" ")[0]
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom))
            except: pass

        
        description = " ".join(response.xpath("//div[@id='property_description']//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        energy_label = response.xpath("//li[contains(.,'EPC')]//text()").get()
        if energy_label: 
            item_loader.add_value("energy_label", energy_label.split(" ")[-1])
        
        import dateparser
        available_date = response.xpath("//span[contains(@class,'available')]/text()").get()
        if available_date:
            available_date = available_date.split(" ")[-1]
            if "now" not in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        floor_plan_images = [x for x in response.xpath("//div[contains(@class,'floorplan')]//@data-src-lg").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]//text() | //span[contains(@class,'available')]/text()[contains(.,' furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('add_marker')[-1].split(';')[0].split(",")[1].replace('"',"")
            longitude = latitude_longitude.split('add_marker')[-1].split(';')[0].split(",")[2].replace('"',"")
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        images = [x for x in response.xpath("//div[contains(@class,'image_carousel__main__slide')]//@data-src-lg").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Lauristons")
        item_loader.add_value("landlord_phone", "020 7978 5800")
        item_loader.add_value("landlord_email", "lettings.battersea@lauristons.com")
        
        if "short let" in description.lower():
            return
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None