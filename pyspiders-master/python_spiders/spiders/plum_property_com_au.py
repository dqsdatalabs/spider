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
    name = 'plum_property_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    url = "https://www.plum-property.com.au/property-search-result-ajax/"
    headers = {
        'authority': 'www.plum-property.com.au',
        'accept': '*/*',
        'x-requested-with': 'XMLHttpRequest',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.plum-property.com.au',
        'referer': 'https://www.plum-property.com.au/property-search-result/',
        'accept-language': 'tr,en;q=0.9',
    }
    formdata = {
        'lastid': '1',
        'vdev': '0.33179231961499633',
        'prop_mode': 'for_rent',
        'prop_type': '',
        'current_page_id': '565'
    }

    def start_requests(self):
        queries = [
            {
                "query": ["Unit"],
                "property_type": "apartment",
            },
            {
                "query": ["House","Townhouse"],
                "property_type": "house",
            },
        ]
        for item in queries:
            for query in item.get("query"):
                self.formdata["prop_type"] = query
                yield FormRequest(self.url,
                            callback=self.parse,
                            headers=self.headers,
                            formdata=self.formdata,
                            dont_filter=True,
                            meta={'property_type': item.get('property_type'), 'prop_type':query})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='item']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page == 2 or seen:
            self.formdata["prop_type"] = response.meta["prop_type"]
            self.formdata["lastid"] = str(page)
            yield FormRequest(self.url,
                                callback=self.parse,
                                headers=self.headers,
                                formdata=self.formdata,
                                dont_filter=True,
                                meta={'property_type':response.meta["property_type"], 'prop_type':response.meta["prop_type"], 'page':page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Plum_Property_Com_PySpider_australia")          
        
        title = "".join(response.xpath("//h1[@itemprop='address']/span[@class!='subhd']/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
        item_loader.add_xpath("city", "//h1[@itemprop='address']/span[@itemprop='addressRegion']/text()")
        item_loader.add_xpath("room_count", "//li[div[@class='bed-icon']]/text()")
        item_loader.add_xpath("bathroom_count", "//li[div[@class='bath-icon']]/text()")
        rent = response.xpath("//h1[@itemprop='address']/span[@class='subhd']/text()[not(contains(.,'APPLICATION') or contains(.,'Application'))]").get()
        if rent:        
            if "per week" in rent:
                rent = rent.split("$")[-1].split("–")[-1].lower().split('p')[0].strip().replace(',', '')
                item_loader.add_value("rent", int(float(rent)) * 4)
            else:
                rent = rent.split("$")[-1].lower().strip().replace(',', '')
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency", 'USD')
    
        parking = response.xpath("//li[div[@class='car-icon']]/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//div[@class='property-info']//div[@class='col-md-4']//text()[contains(.,' balcony ')]").get()
        if balcony:
            item_loader.add_value("balcony", True) 
            
        furnished = response.xpath("//div[@class='property-info']//div[@class='col-md-4']//text()[contains(.,'furnished') or contains(.,'Furnished') ]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False) 
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True) 
        pets = response.xpath("//div[@class='property-info']//div[@class='col-md-4']//text()[contains(.,'pets') or contains(.,'Pet friendly')]").get()
        if pets:
            if "no " in pets.lower():
                item_loader.add_value("pets_allowed", False) 
            else:
                item_loader.add_value("pets_allowed", True) 

        description = " ".join(response.xpath("//div[@class='property-info']//div[@class='col-md-4']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        latlng = response.xpath("//script[contains(.,'lng:') and contains(.,'lat:')]/text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split("lat: ")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng: ")[1].split("}")[0].strip())
   
        item_loader.add_xpath("landlord_name", "//div[@class='agent-profile']//h3/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-profile']//a[i[contains(@class,'phone-square-icon')]]/text()")
        item_loader.add_xpath("landlord_email", "substring-after(//div[@class='agent-profile']//a[contains(@href,'mailto')]/@href,'mailto:')")
        
        image = "".join(response.xpath("//script[contains(.,'openPhotoSwipe')]/text()").getall())
        if image:
            try:
                img ="[" + image.strip().split("var items = [")[1].strip().split("];")[0].strip()+"]"
                img = re.sub("\s{2,}", " ", img)
                i = img.replace('src:','"src":').replace('w:','"w":').replace('h:','"h":').replace(", }","}").replace("'",'"')
                json_image = json.loads(i)
            except:
                img ="[" + image.strip().split("var items = [")[1].strip().split("html")[0].strip()+"]"
                img = re.sub("\s{2,}", " ", img)
                i = img.replace(", {]","]").replace('src:','"src":').replace('w:','"w":').replace('h:','"h":').replace(", }","}").replace("'",'"')
                json_image = json.loads(i)

            if json_image:
                images = [x["src"] for x in json_image]
                if images:
                    item_loader.add_value("images", images)

        yield item_loader.load_item()
   
     