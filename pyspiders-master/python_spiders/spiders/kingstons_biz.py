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
    name = 'kingstons_biz'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'           
   
    def start_requests(self):
        start_url = "http://www.kingstons.biz/properties?eapowquicksearch=1&limitstart=0"
        payload = 'tx_placename=&filter_rad=5&filter_keyword=&filter_cat=2&filter_stype=4&filter_beds=&filter_baths=&filter_price_low=&filter_price_high=&commit=&filter_lat=0&filter_lon=0&filter_location=%5Bobject%2BObject%5D'
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'http://www.kingstons.biz',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'http://www.kingstons.biz/',
            'Accept-Language': 'tr,en;q=0.9',
        }
        yield Request(start_url, method="POST", callback=self.parse, headers=headers, body=payload)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@id,'listing')]//a[contains(.,'Read more')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//div[@id='propdescription']//div[contains(@class,'eapow-desc-wrapper')]/p//text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else:
                property_type = response.xpath("//li[contains(.,'House') or contains(.,'cottage') or contains(.,'home')]//text()").get()
                if property_type:
                    item_loader.add_value("property_type", "house")
                else: return
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Kingstons_PySpider_united_kingdom")    
        title = response.xpath("//div/h1/text()[normalize-space()]").extract_first()
        if title:
            item_loader.add_value("title",title.strip())   
            item_loader.add_value("city",title.split(",")[-1].strip())   
        city_zipcode = response.xpath("//div[@id='propdescription']//div[contains(@class,'eapow-mainaddress')]/address/text()").extract_first()
        if city_zipcode:
            zipcode = city_zipcode.strip().split(" ")[-2]+" "+city_zipcode.strip().split(" ")[-1]
            item_loader.add_value("zipcode",zipcode.strip()) 
        address =", ".join(response.xpath("//div[@id='propdescription']//div[contains(@class,'eapow-mainaddress')]//text()").extract())
        if address:
            item_loader.add_value("address",address.strip())      
        
        item_loader.add_xpath("bathroom_count", "//div[@id='PropertyRoomsIcons']//img[contains(@src,'bathroom')]/following-sibling::strong[1]/text()[.!='0']")
                
        external_id = response.xpath("//div[@id='propdescription']//div[@class='eapow-sidecol' and b[.='Ref #']]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.replace(":","").strip()) 
        room_count = response.xpath("//div[@id='PropertyRoomsIcons']//img[contains(@src,'bedroom')]/following-sibling::strong[1]/text()[.!='0']").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())

        rent = response.xpath("//small[@class='eapow-detail-price']//text()").extract_first()
        if rent:     
            item_loader.add_value("rent_string",rent) 
        available_date = response.xpath("//ul[@id='starItem']/li[contains(.,'Available')]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        parking = response.xpath("//ul[@id='starItem']/li[contains(.,'Parking') or contains(.,'Garage') or contains(.,'parking')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        energy_label = response.xpath("//ul[@id='starItem']/li[contains(.,'Energy Rating')]//text()").get()
        if energy_label:        
            energy_label = energy_label.split("Rating")[1].strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label",energy_label)
  
        furnished = response.xpath("//ul[@id='starItem']/li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()  
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//div[@id='propdescription']//div[contains(@class,'eapow-desc-wrapper')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        latlng = response.xpath("//script/text()[contains(.,'lat:') and contains(.,'lon: ') ]").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('lat: "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latlng.split('lon: "')[1].split('"')[0].strip()) 
        images = [response.urljoin(x) for x in response.xpath("//div[@id='eapowgalleryplug']//a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='eapowfloorplanplug']//a/img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)     
        landlord_name = response.xpath("//div[@id='propdescription']//div[@id='DetailsBox']//a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_value("landlord_name", "Kingstons")
            item_loader.add_value("landlord_phone", "01225 709115")
        item_loader.add_xpath("landlord_phone", "//div[@id='propdescription']//div[contains(@class,'sidecol-phone')]/text()")
        item_loader.add_value("landlord_email", "lettings@kingstons.biz")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None