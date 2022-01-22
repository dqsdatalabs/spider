# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'relocation-amsterdam_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "RelocationAmsterdam_PySpider_netherlands"
    headers={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cookie": "PHPSESSID=ojotvup77lkjhnb9fe7p89c5jr",
        "Host": "www.relocation-amsterdam.nl",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Mobile Safari/537.36",
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.relocation-amsterdam.nl/properties/",
                ],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,headers=self.headers
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        
        for item in response.xpath("//ul[@class='woning_box']//li/@onclick").getall():
            follow_url = response.urljoin(item.split('href="')[-1].replace('"',""))
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page or seen:
            nextpage=f"https://www.relocation-amsterdam.nl/properties/?action=search&city=&min-price=&max-price=&page={page}"
            if nextpage:
                yield Request(nextpage, callback=self.parse)
            

  
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)

        city=response.xpath("//strong[.='City:']/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city)
        rent=response.xpath("//strong[.='Price:']/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace(".",""))
        item_loader.add_value("currency","EUR")
        furnished=response.xpath("//strong[.='Decoration:']/following-sibling::text()").get()
        if furnished and "Unfurnished" in furnished:
            item_loader.add_value("furnished",True)
        if furnished and "Furnished" in furnished:
            item_loader.add_value("furnished",True)
        square_meters=response.xpath("//strong[.='Surface:']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        available_date=response.xpath("//strong[.='Available at:']/following-sibling::text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        description=response.xpath("//div[@class='house_left']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//a[@class='pararius_image']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Relocation Amsterdam")
        item_loader.add_value("landlord_email","info@relocation-amsterdam.nl")
        item_loader.add_value("landlord_phone","+31 (0) 20 - 331 7373")
        yield item_loader.load_item()