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
from word2number import w2n
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'spacelet_net'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    handle_httpstatus_list = [401]
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        "PROXY_TR_ON": True,
        # "PROXY_ON": True,
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
    }
    headers = {
            'authority': 'spacelet.net',
            'upgrade-insecure-requests': '1',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            # 'accept-language': 'tr,en;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
    def start_requests(self):
        
        yield Request("https://spacelet.net/our-properties/", headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//h3[contains(@class,'product-title')]/a"):
            title = item.xpath("./strong/text()").get()
            address = item.xpath("./p/text()").getall()
            zipcode = item.xpath("./p/br/following-sibling::text()").get()
            addr = ''
            for a in address:
                addr += a.strip() + ' '
            if "Rent" in title:
                f_url = response.urljoin(item.xpath("./@href").get())
                yield Request(
                    f_url, 
                    callback=self.populate_item,
                    headers=self.headers,
                    meta={"address" : addr, "zipcode" : zipcode},
                )
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                headers=self.headers,
                callback=self.parse,
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//h1//strong/text()").get()

        title =  response.xpath("//div[@class='summary-container']/h1/strong/text()").extract_first()
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else: return
            
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Spacelet_PySpider_"+ self.country + "_" + self.locale)
                
        title = "".join(response.xpath("//div[@class='summary-container']/h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.meta.get("address")
        if address:
            item_loader.add_value("address", address.strip())
            city = " ".join(address.split(',')[-1].strip().split(" ")[:-2])
            item_loader.add_value("city", city.split('/')[0].strip())
        # city = response.xpath("//h1[@itemprop='name']//p/text()[1]").get()
        # if city: item_loader.add_value("city", city.split(',')[-1].strip())

        description = response.xpath("//h3[contains(.,'description')]/following-sibling::p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", re.sub('\s{2,}', ' ', filt.text))

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters = str(int(float(square_meters.replace(',', '.')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)
        if response.xpath(
            "//h1/strong[contains(.,'Studio') or contains(.,'Bedsit') or contains(.,'One') or contains(.,'Double') or contains(.,'Room')]/text()"
            ).get():
            item_loader.add_value("room_count","1")
        elif response.xpath(
            "//h1/strong[contains(.,'Two Bedroom')]/text()"
            ).get():
            item_loader.add_value("room_count","2")
        elif 'bedroom' in desc_html.lower():
            room_count = desc_html.lower().split('bedroom')[0].strip().replace('\xa0', '').split(' ')[-1].strip()
            try:
                room_count = str(int(float(w2n.word_to_num(room_count))))
                item_loader.add_value("room_count", room_count)
            except:
                pass
        if "terrace" in desc_html.lower():
            item_loader.add_value("terrace", True)
        if "swimming pool" in desc_html.lower():
            item_loader.add_value("swimming_pool", True)
        if "washing machine" in desc_html.lower():
            item_loader.add_value("washing_machine", True)
        if "dishwasher" in desc_html.lower():
            item_loader.add_value("dishwasher", True)
            
        rent = response.xpath("//p[@class='price']/preceding-sibling::h6/text()").get()
        if rent:
            rent = rent.split('pcm')[0].split('/')[-1].split('£')[1].strip().replace('\xa0', '')
            rent = rent.replace(',', '').replace('.', '').replace(' ', '')
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'GBP')

        zipcode = response.meta.get("zipcode")
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        furnished = response.xpath("//li[contains(.,'Furnished')]/text()").get()
        if furnished: 
            item_loader.add_value("furnished", True)
        parking = response.xpath("//li[contains(.,'parking')]/text() | //p/text()[contains(.,'parking')]").get()
        if parking: 
            item_loader.add_value("parking", True)
        images = [x for x in response.xpath("//figure//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        if 'furnished' in desc_html.lower():
            furnished = desc_html.lower().split('furnished')[0].strip().replace('\xa0', '').split(' ')[-1].strip()
            if 'fully' in furnished.lower():
                item_loader.add_value("furnished", True)
        item_loader.add_value("landlord_name","SPACELET PROPERTY FINDERS")
        landlord_phone = response.xpath("//strong[contains(.,'TELEPHONE')]/following-sibling::text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else: item_loader.add_value("landlord_phone", "")
        
        item_loader.add_value("landlord_email","info@spacelet.net")

        if not item_loader.get_collected_values("description"):
            description = " ".join(response.xpath("//div[h3[contains(.,'description')]]/following-sibling::*//text()").getall()).strip()
            if description: item_loader.add_value("description", description)
        
        if not response.xpath("//div[contains(@class,'gallery-wrapper')]//span[contains(text(),'Let Agreed')]").get(): yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower() or "conversion" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None