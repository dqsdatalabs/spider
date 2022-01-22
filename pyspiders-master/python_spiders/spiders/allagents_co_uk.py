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
    name = 'allagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    handle_httpstatus_list = [500,400]
    custom_settings = {
    #   "PROXY_ON": True,
      "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
      "HTTPCACHE_ENABLED": False
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.allagents.co.uk/properties/to-rent/london/?view=list&added=shortterm&added=longterm&added=student&proptype=flats&page=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.allagents.co.uk/properties/to-rent/london/?view=list&added=shortterm&added=longterm&added=student&proptype=houses&page=1",
                    "https://www.allagents.co.uk/properties/to-rent/london/?view=list&added=shortterm&added=longterm&added=student&proptype=bungalows&page=1"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        total_page = response.xpath("//a[contains(.,'Last')]/text()").get()
        if total_page:
            total_page = total_page.split("(")[1].split(")")[0].strip()
        for item in response.xpath("//div[contains(@class,'panel-property')]"):
            follow_url = response.urljoin(item.xpath(".//div[@class='property-information']/a/@href").get())
            status = item.xpath(".//p[contains(@class,'label-success')]//text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
                
        if total_page:
            if page<int(total_page):
                headers = {
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
                }
                f_url = response.url.replace(f"&page={page-1}", f"&page={page}")
                yield Request(f_url, callback=self.parse, headers=headers, meta={"page": page+1})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        title = "".join(response.xpath("//h1/text() | //h3[contains(.,'Description')]/following-sibling::p//text()").getall())
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            if get_p_type_string(response.url):
                item_loader.add_value("property_type", get_p_type_string(response.url))
            else: return
        item_loader.add_value("external_source", "Allagents_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("id/")[1].split("/")[0])

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'mainData')]//span[contains(@class,'pull-left')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        city = response.xpath("//script[contains(.,'itemListElement')]//text()").get()
        if city:
            city = city.split('position": 2')[1].split('name": "')[1].split('"')[0].strip()
            item_loader.add_value("city", city)
        
        # zipcodecheck=item_loader.get_output_value("zipcode")
        # if not zipcodecheck:
        #     zipcode1=response.xpath("//title//text()").get()
        #     zipcode1=zipcode1.split("|")[0].split(",",2)
        #     item_loader.add_value("zipcode",zipcode1[2]) 
        zipcode=response.xpath("//title//text()").get()
        if zipcode:
            zipcode="".join(zipcode.split("|")[0].split(",")[-2:])
            item_loader.add_value("zipcode",zipcode.strip())


                

        rent = response.xpath("//div[contains(@class,'mainData')]//span[contains(@class,'prices desktop')]//text()").get()
        if rent:            
            if "annually" in rent.lower():
                return
            if "pcm" in rent:
                rent = rent.split("£")[-1].split("pcm")[0].strip().replace(",","")
            else:
                rent = rent.split("£")[-1].strip().split(" ")[0].replace(",","")
            if rent =="0":
                return
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//li[contains(@class,'information')][contains(.,'Deposit')]//text()").get()
        if deposit:
            deposit = deposit.split("£")[1].strip().replace(",","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@id,'full-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//h1//text()").get()
        if room_count:
            room_count = room_count.lower().strip().split(" ")[0]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                room_count = response.xpath('//li[contains(.,"bedrooms")]/text()').get()
                if room_count and ('two' in room_count.lower()):
                    item_loader.add_value("room_count", "2")

        bathroom_count = response.xpath("//li[contains(@class,'feature')][contains(.,'Bathroom')]//text()").get()
        if bathroom_count:
            try:
                if "/" in bathroom_count:
                    bathroom_count = bathroom_count.split("/")[1]
                bathroom_count = bathroom_count.lower().split("bathroom")[0].replace("full","").replace("modern","").replace("luxury","").replace("piece","").replace("fully","").replace("tiled","").strip()
                if bathroom_count.isdigit():
                    item_loader.add_value("bathroom_count", bathroom_count)
                else:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except:
                pass
        
        images = [x.split("url('")[1].split("'")[0] for x in response.xpath("//div[contains(@class,'images')]//@style[contains(.,'background-image')]").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@id,'floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime 
        import dateparser
        available_date = "".join(response.xpath("//li[contains(@class,'feature')][contains(.,'Available ')]//text()").getall())
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        else:
            available_date = "".join(response.xpath("//li[contains(@class,'information')][contains(.,'Available ')]//text()").getall())
            if available_date:
                available_date = available_date.split(":")[1].strip()
                if not "now" in available_date.lower():
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        datecheck=item_loader.get_output_value("available_date")
        if not datecheck:
            date1= " ".join(response.xpath("//div[contains(@id,'full-description')]//text()").getall())
            if date1:
                fdate=date1.split("AVAILABLE")[-1].split("!")[0].strip()
            
                if fdate:
                    if not "now" in fdate.lower():
                        date_parsed = dateparser.parse(fdate.lower(), date_formats=["%d/%m/%Y"])
                        if date_parsed:
                            date2 = date_parsed.strftime("%Y-%m-%d")
                            item_loader.add_value("available_date", date2)



        parking = response.xpath("//li[contains(@class,'feature')][contains(.,'Parking') or contains(.,'Garage') or contains(.,'PARKING') or contains(.,'GARAGE') or contains(.,'parking') or contains(.,'garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(@class,'feature')][contains(.,'Balcon') or contains(.,'balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(@class,'feature')][contains(.,'Furnished')]//text()[not(contains(.,'Unfurnished'))] | //li[contains(@class,'information')][contains(.,'Furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//li[contains(@class,'feature')][contains(.,'Floor')]//text()[not(contains(.,'Floors'))]").get()
        if floor and "floors" not in floor.lower():
            floor = floor.split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//li[contains(@class,'feature')][contains(.,'EPC')]//text()").get()
        if energy_label:
            energy_label = energy_label.lower().replace("rating","").replace("epc","").replace("-","").replace(":","").strip()
            item_loader.add_value("energy_label", energy_label)
            
        dishwasher = response.xpath("//li[contains(@class,'feature')][contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//li[contains(@class,'feature')][contains(.,'Washing machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        latitude_longitude = response.xpath("//script[contains(.,'var lat =')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('var lat =')[1].split(';')[0]
            longitude = latitude_longitude.split('var lng =')[1].split(';')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'panel-body')]//p[contains(.,'This property is marketed by:')]//following-sibling::img//@alt").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            name = response.xpath("substring-before(//p[@class='text-bold']/a/text(),'-')").get()
            item_loader.add_value("landlord_name", name.strip())
        
        landlord_phone = response.xpath("//div[contains(@class,'panel-body')]//strong//text()").get()
        if landlord_phone and landlord_phone.replace(" ","").isdigit():
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "0161 834 8340")

        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None