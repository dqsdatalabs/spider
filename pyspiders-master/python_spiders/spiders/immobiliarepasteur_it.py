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
    name = 'immobiliarepasteur_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Immobiliarepasteur_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.immobiliarepasteur.it/Propriet%C3%A0/?var_ptype=33&var_location=0&var_status=45&bedrooms=any&minprice=any&maxprice=any&propertys=dbcc9df9a3",
                ],
                "property_type": "apartment"
            },
	        { 
                "url": [
                    "http://www.immobiliarepasteur.it/Propriet%C3%A0/?var_ptype=34&var_location=0&var_status=45&bedrooms=any&minprice=any&maxprice=any&propertys=dbcc9df9a3",
                    "http://www.immobiliarepasteur.it/Propriet%C3%A0/?var_ptype=35&var_location=0&var_status=45&bedrooms=any&minprice=any&maxprice=any&propertys=dbcc9df9a3",
                    "http://www.immobiliarepasteur.it/Propriet%C3%A0/?var_ptype=70&var_location=0&var_status=45&bedrooms=any&minprice=any&maxprice=any&propertys=dbcc9df9a3",
                    "http://www.immobiliarepasteur.it/Propriet%C3%A0/?var_ptype=67&var_location=0&var_status=45&bedrooms=any&minprice=any&maxprice=any&propertys=dbcc9df9a3",
                    "http://www.immobiliarepasteur.it/Propriet%C3%A0/?var_ptype=69&var_location=0&var_status=45&bedrooms=any&minprice=any&maxprice=any&propertys=dbcc9df9a3"
                ],
                "property_type": "house"
            },
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
        for item in response.xpath("//a[contains(.,'Dettagli')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//li[@class='next']//@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//a[@class='yellow-btn']/text()").get()
        if rent:
            rent=rent.split("€")[-1] 
            item_loader.add_value("rent",rent)
        desc=" ".join(response.xpath("//div[@class='property-desc']//text()").getall())
        if desc:
            desc=desc.replace("\t","").replace("\n","")
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        square_meters=response.xpath("//li[contains(.,'MQ')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
        room_count=response.xpath("//li[contains(.,'Locali')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        bathroom_count=response.xpath("//li[contains(.,'Bagni')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0])
        images=[x for x in response.xpath("//div[@class='image-wrapper']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        elevator=response.xpath("//li[@class='single-feature']//a//text()[.='Ascensore']").get()
        if elevator:
            item_loader.add_value("elevator",True)
        name=response.xpath("//div[@class='desc-box']/h4/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        else:
            item_loader.add_value("landlord_name","immobiliare pasteur")

        email=" ".join(response.xpath("//div[@class='desc-box']//p[@class='person-email']//text()").getall())
        if email:
            item_loader.add_value("landlord_email",email.replace("\t","").replace("\n",""))
        else:
            item_loader.add_value("landlord_email","immobiliare@immobiliarepasteur.it")

        phone=" ".join(response.xpath("//p[@class='person-number']//text()").getall())
        if phone:
            item_loader.add_value("landlord_phone",phone.replace("\t","").replace("\n",""))
        else:
            item_loader.add_value("landlord_phone","0249630479")


        item_loader.add_value("currency","EUR")
        external_id = response.xpath("//span[contains(text(),'RIF')]/text() | //p[contains(text(),'RIF')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[-1].strip()
            item_loader.add_value("external_id",external_id)
        else:
            parags = str(response.xpath("//p/text()").getall())
            if re.search("RIF:[A-Z-]+",parags):
                external_id = re.search("RIF:[A-Z-]+",parags)[0]
                external_id = external_id.split(":")[-1].strip()
                item_loader.add_value("external_id",external_id)
            else:
                id = response.xpath("//link[@rel='shortlink']/@href").get()
                if id:
                    id = id.split("p=")[1]
                    item_loader.add_value("external_id",id)

        if "vendita" in response.url:
            return

        item_loader.add_value("city","Milan")
        item_loader.add_value("address","Milan")
        yield item_loader.load_item()