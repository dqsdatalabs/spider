# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import re
from word2number import w2n

class MySpider(Spider):
    name = 'regent_property_com'  # LEVEL 1
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = ['https://regent-property.com/property-search/page/1/?type=to-let&area_search&minprice=0&maxprice=15001&minbeds=0&maxbeds=11&view=grid&order=desc&apf_search=go']  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='apf__properties']/article"):
            href = item.xpath("./a/@href").get()
            follow_url = response.urljoin(href)
            yield Request(follow_url.strip('/') + '/?tab=map', callback=self.get_latlong, meta={"follow_url": follow_url})
            seen = True
        
        if page == 2 or seen:
            url = f"https://regent-property.com/property-search/page/{page}/?type=to-let&area_search&minprice=0&maxprice=15001&minbeds=0&maxbeds=11&view=grid&order=desc&apf_search=go"
            yield Request(url, callback=self.parse, meta={"page": page+1})
    
    def get_latlong(self, response):

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        latitude = ''
        longitude = ''
        if latitude_longitude:
            latitude = latitude_longitude.split('var latitude = parseFloat("')[1].split('"')[0].strip()
            longitude = latitude_longitude.split('var longitude = parseFloat("')[1].split('"')[0].strip()
        yield Request(response.meta.get('follow_url').strip('/') + '/?tab=floorplan', callback=self.get_floorplan, meta={"property_type": response.meta.get('property_type'), "follow_url": response.meta.get('follow_url'), "latitude": latitude, "longitude": longitude})

    def get_floorplan(self, response):

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//h2[contains(.,'Floorplan')]/../ul//img/@src").getall()]
        yield Request(response.meta.get('follow_url'), callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "latitude": response.meta.get('latitude'), "longitude": response.meta.get('longitude'), "floor_plan_images": floor_plan_images})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        f_text = response.url
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@class='apf__single__property__content']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Regentproperty_PySpider_" + self.country + "_" + self.locale)
      
        title = response.xpath("//h1/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            
            zipcode = title.split(',')[-1].strip().split(' ')
            zipcode = ' '.join(z for z in zipcode if not z.isalpha()).strip()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)

        item_loader.add_value("city", "London")
        
        external_id = response.xpath("//script[contains(.,'property_id')]/text()").get()
        if external_id:
            external_id = external_id.split("_id = '")[1].split("'")[0]
            item_loader.add_value("external_id", external_id)
        
        bathroom_count = response.xpath("//span[contains(@class,'flaticon-bath')]/following-sibling::text()").get()
        if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        if not item_loader.get_collected_values("bathroom_count"):
            bathroom_count = response.xpath("//h2[contains(.,'Main features')]/..//p[contains(.,'bathroom')]/text()").get()
            if bathroom_count:
                try:
                    bathroom_count = w2n.word_to_num(bathroom_count.lower().split('bathroom')[0].strip())
                    item_loader.add_value("bathroom_count", str(bathroom_count))
                except:
                    pass

        latitude = response.meta.get('latitude')
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.meta.get('longitude')
        if longitude:
            item_loader.add_value("longitude", longitude)
        floor_plan_images = response.meta.get('floor_plan_images')
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//img[contains(@src,'graphtype=EPC')]/@src").get()
        if energy_label:
            energy_label = int(float(energy_label.split('currentenergy=')[-1].split('&')[0].strip()))
            if energy_label >= 92:
                item_loader.add_value("energy_label", 'A')
            elif energy_label <= 91 and energy_label >= 81:
                item_loader.add_value("energy_label", 'B')
            elif energy_label <= 80 and energy_label >= 69:
                item_loader.add_value("energy_label", 'C')
            elif energy_label <= 68 and energy_label >= 55:
                item_loader.add_value("energy_label", 'D')
            elif energy_label <= 54 and energy_label >= 39:
                item_loader.add_value("energy_label", 'E')
            elif energy_label <= 38 and energy_label >= 21:
                item_loader.add_value("energy_label", 'F')
            elif energy_label <= 20 and energy_label >= 1:
                item_loader.add_value("energy_label", 'G')
        
        floor = response.xpath("//h2[contains(.,'Main features')]/..//p[contains(.,'floor')]/text()").get()
        if floor:
            try:
                floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0].strip().split(' ')[-1]))
                item_loader.add_value("floor", floor)
            except:
                pass
                
        elevator = response.xpath("//h2[contains(.,'Main features')]/..//p[contains(.,'Lift') or contains(.,'lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        rent = response.xpath("//div[@class='digits']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))    
    
        room_count = response.xpath("//li/span[contains(@class,'flaticon-bed')]/following-sibling::text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//li/span[contains(@class,'flaticon-sofa')]/following-sibling::text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        desc = "".join(response.xpath("//h2[contains(.,'property')]/following-sibling::p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        square_meters = response.xpath("//div/p[contains(.,'sq')]//text()[.='picturesque']").get()
        unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|sq.|sq)",desc.replace(",",""))
        if square_meters:
            if "ft" in square_meters:
                square_meters = square_meters.split('sq')[0].strip().replace(',', '')
                sqm = str(int(float(square_meters)* 0.09290304))
                item_loader.add_value("square_meters", sqm)
            else:
                sqm = int(float(square_meters.split('sq')[0].strip().replace(',', '.')))
                item_loader.add_value("square_meters", sqm)
        
        elif unit_pattern:   
            if unit_pattern:
                sqm = str(int(float(unit_pattern[0][0]) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        
        swimming_pool = response.xpath("//div/p[contains(.,'swimming pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)       

        balcony = response.xpath("//div/p[contains(.,'balcon') or contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)   
        furnished = response.xpath("//div/p[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True) 
        parking = response.xpath("//div/p[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True) 

        terrace = response.xpath("//div/p[contains(.,'terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True) 
      
        images = [x for x in response.xpath("//div[@class='property__gallery']/div/@data-src").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("landlord_email", "info@regent-property.com")
        item_loader.add_value("landlord_name", "Regent Property")
        item_loader.add_value("landlord_phone", "2087439101")
          
        status = response.xpath("//div[@class='status']/text()").get()
        if not status:
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terraced" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None