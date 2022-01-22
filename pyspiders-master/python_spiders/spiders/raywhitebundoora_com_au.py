# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# from tkinter.font import ROMAN
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'raywhitebundoora_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Raywhitebundoora_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "cookie": "_gcl_au=1.1.166634716.1642054027; _ga=GA1.3.849181805.1642054029; _gid=GA1.3.2008580496.1642054029; __adroll_fpc=ed14deeff3e80be4dca50b8cf152647d-1642054038866; _fbp=fb.2.1642054044557.1257397118; __ar_v4=ZX5C2L42ZBB4BBSL3MMBKA%3A20220112%3A3%7CGLWESRX7ZVGT5FHMFJ5WGT%3A20220112%3A3%7CHL5SZOOIARHTXAH7NGKASM%3A20220112%3A3",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Mobile Safari/537.36",
        }

        start_urls = [
            {"url": "https://raywhitebundoora.com.au/properties/residential-for-rent?category=&keywords=&minBaths=0&minBeds=0&minCars=0&rentPrice=&sort=updatedAt+desc&suburbPostCode="},

        ] 
         # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,headers=headers)
    # 1. FOLLOWING
    def parse(self, response):
        border=4
        page = response.meta.get("page", 2)
        seen = False
        if "links" in str(response.body) and "value" in str(response.body) and "data" in str(response.body) and "hits" in str(response.body):
            data=json.loads(response.body)['data']
            for item in data:
                follow_url = str(item['value']['links']).split("url': '")[-1].split("',")[0]
                yield Request(follow_url, callback=self.populate_item)
            seen=True
        else:
            for item in response.xpath("//a[@class='proplist_item_image_wrap']/@href").getall():
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item)
        if page == 2 or seen:
            if page<border:
                headers = {
                    "accept": "*/*",
                    "accept-encoding": "gzip, deflate, br",
                    "accept-language": "en-US,en;q=0.9,tr;q=0.8",
                    "content-type": "application/json"
                }
                payload = {"size":50,"statusCode":"CUR","typeCode":"REN","sort":["updatedAt desc","id desc","_score desc"],"organisationId":[112],"from":50}
                yield Request(
                    "https://raywhiteapi.ep.dynamics.net/v1/listings?apiKey=FB889BB8-4AC9-40C2-829A-DD42D51626DE",
                    callback=self.parse,
                    body=json.dumps(payload),
                    method="POST",
                    headers=headers,
                    dont_filter=True,
                    meta={'page':page+1}
                )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres="".join(response.xpath("//h1[@class='pdp_address ']//text()").getall())
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//span[@class='pdp_price']/text()").get()
        if rent:
            a=rent.split("/")
            for i in a:
                if "pcm" in i:
                    item_loader.add_value("rent",rent.split("pcm")[0].split("$")[-1].replace(".",""))
        item_loader.add_value("currency","USD")
        room_count=response.xpath("//li[@class='bed']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//li[@class='bath']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        property_type=response.xpath("//span[@class='pdp_descriptor muted']/text()").get()
        if property_type and "House" in property_type:
            item_loader.add_value("property_type","house")
        images=[x for x in response.xpath("//picture//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//div[@class='pdp_description_content']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        name=response.xpath("//h4[@class='echo']/a/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//span[@class='clickable']/span/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)
            

        yield item_loader.load_item()