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
    name = 'realo_be'
    execution_type='testing'
    country='belgium' 
    locale='en'
    custom_settings={"HTTPCACHE_ENABLED":False}
    def start_requests(self):

        start_urls = [  
            {
                "url" : [
                    "https://www.realo.be/en/search/flat/for-rent?page=1",
                ],
                "property_type" : "apartment",
            }, 
            {
                "url" : [
                    "https://www.realo.be/en/search/house/for-rent?page=1",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.realo.be/en/search/room/for-rent?page=1",
                ],
                "property_type" : "house",
            },
        ]
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,headers=headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        rent_via = response.xpath("//li[@class='col module module-address']/div/strong/text()[contains(.,'via Realo')]").extract()
        if  rent_via: 
            return
        else: 
            
            page = response.meta.get("page", 2)
            seen=False
            max_page = response.xpath("//li[contains(@class,'pagination-list-item')][last()]/a/text()").get()
            max_page = int(max_page) if max_page else -1

            for item in response.xpath("//div[@data-id='body']/a[@data-id='link']/@href").getall():
                yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
                seen = True
            if page <= max_page or seen: 
                follow_url = response.url.replace("page=" + str(page - 1), "page=" + str(page))
                yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page": page + 1})
 
    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)

        rent_via = response.xpath("//li[@class='col module module-address']/div/strong/text()[contains(.,'via Realo')]").extract()
        if rent_via:
            return

        if "search" in response.url or "3137820" in response.url or "35118" in response.url: 
            return
        rented = "".join(response.xpath("//div[@class='type']//text()[contains(.,'Not for sale or to rent')] | //div[strong[contains(.,'sale')]]").extract())
        if rented:
            return

        commercial = response.xpath("//div[contains(@class,'description')]//p//text()").get()
        if not commercial or "restaurant" in commercial.lower():
            return

        prop = "".join(response.xpath("//tbody/tr[td[.='Property type']]/td[2]/text()").extract())
        if "flat" in prop.lower():
            item_loader.add_value("property_type", "apartment")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)


        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Realo_PySpider_belgium", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1[@class='address']/text()", input_type="M_XPATH")
        address = "".join(response.xpath("//h1[@class='address']/text()").getall())
        if address:
            city = address.split(",")[-1].strip()
            if " " in city:
                zipcode = address.split(",")[-1].strip().split(" ")[0]
                if zipcode.isdigit():
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.split(",")[-1].split(zipcode)[1].strip())
            else:
                item_loader.add_value("city", city)
                zipcode = "" 
                try:
                    zipcode = address.split(",")[-2].strip().split(" ")[0]
                except: zipcode = ""
                if zipcode and zipcode.isdigit():
                    item_loader.add_value("zipcode", zipcode)
        zipcheck=item_loader.get_output_value("zipcode")
        if not zipcheck:
            zips=response.xpath("//li//a[@itemprop='url']/@href").getall()
            for i in zips:
                a=re.findall("\d+",i) 
                if a:
                    item_loader.add_value("zipcode",a)
                    continue
        address1=response.xpath("//h1/span[@class='type']/following-sibling::text()").get()
        if address1:

            address1=address1.replace("\n","").replace("\t","").strip()          
            item_loader.add_value("address",address1)

                    
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//tr/td[contains(.,'Habitable')]/following-sibling::td/text() | //tr/td[contains(.,'depth')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        
        if response.xpath("//tr/td[contains(.,'Bedroom')]/following-sibling::td/text()[.!='0']"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//tr/td[contains(.,'Bedroom')]/following-sibling::td/text()[.!='0']", input_type="F_XPATH", get_num=True)
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[contains(@class,'listings__item')]//div[contains(@class,'col-1') and contains(@class,'bed')]//text()", input_type="M_XPATH", get_num=True, split_list={" ":0})
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//tr/td[contains(.,'Bathroom')]/following-sibling::td/text()[.!='0']", input_type="F_XPATH", get_num=True)
        
        rent = "".join(response.xpath("//div[@class='value']/text()").re(r"\d+")[:2])
        if rent:
            item_loader.add_value("rent", rent)
        # if response.xpath("//li/div[@class='value']/text()[not(contains(.,'No'))]"):
        #     ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li/div[@class='value']/text()", input_type="M_XPATH", get_num=True)
        # else:
        #     ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li/div[contains(@class,'listings__item')]//div[contains(@class,'price')]//text()", input_type="M_XPATH", get_num=True, split_list={"€":1," ":0})
            
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//tr/td[contains(.,'Available from')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"=":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@data-id='gallery']//@src[not(contains(.,'data:image'))]", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div/@data-latlng", input_type="F_XPATH", split_list={"[":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div/@data-latlng", input_type="F_XPATH", split_list={",":1,"]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//tr/td[contains(.,'Floor')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//tr/td[contains(.,'charges')]/following-sibling::td/text()", input_type="M_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Animals allowed')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//tr/td[contains(.,'Parking')]/following-sibling::td/text()[.!='0'] | //ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//td[contains(.,'Terrace') or contains(.,'terrace')] | //ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Pool') or contains(.,'pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Washing') or contains(.,'Washer')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//ul[contains(@class,'list-unstyled grid')]/li[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="REALO", input_type="VALUE")

        energy = response.xpath("//tr/td[contains(.,'Energy')]/following-sibling::td/text()").get()
        energy_label = "".join(response.xpath("//tr/td[contains(.,'EPC value')]/following-sibling::td/text()").getall())
        if energy:
            item_loader.add_value("energy_label", energy.strip())
        elif energy_label:
            item_loader.add_value("energy_label", energy_label.split("(")[1].split(")")[0])
        
        yield item_loader.load_item()