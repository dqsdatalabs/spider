# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'studiosumisura_it'
    external_source = "Studiosumisura_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    start_urls = ['']  # LEVEL 1
    
    headers = {
        'Proxy-Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Proxy-Authorization': 'Basic bWVobWV0a3VydGlwZWtAZ21haWwuY29tOmZCWlVMc3NaZXNGOUx5RERZdW1F',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': 'http://www.studiosumisura.it/',
        'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
        'Cookie': 'ASPSESSIONIDAATBQSDT=PAELPJMABIADIBLDOOGCLAFG; _iub_cs-87909441=%7B%22consent%22%3Atrue%2C%22timestamp%22%3A%222021-09-09T09%3A22%3A09.116Z%22%2C%22version%22%3A%221.2.4%22%2C%22id%22%3A87909441%7D'
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://archivio.studiosumisura.it/easyhome/web/search_embed.asp?vendaff=AFFITTO&comune=&tipo=A&start=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://archivio.studiosumisura.it/easyhome/web/search_embed.asp?vendaff=AFFITTO&comune=&tipo=R&start=1",
                    "http://archivio.studiosumisura.it/easyhome/web/search_embed.asp?vendaff=AFFITTO&comune=&tipo=T&start=1"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "http://archivio.studiosumisura.it/easyhome/web/search_embed.asp?vendaff=AFFITTO&comune=&tipo=Y&start=1",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=self.headers,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='infobutton']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'Avanti')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), headers=self.headers, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("=")[-1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title", "//title/text()")
        
        square_meters = response.xpath("//font[strong[contains(.,'mq')]]/parent::div/parent::td/following-sibling::td//text()").get()
        if square_meters:
            sqm = square_meters.split("m")[0].strip()
            room = square_meters.split("Vani:")[1].strip()
            item_loader.add_value("square_meters", sqm)
            item_loader.add_value("room_count", room)
        
        address = response.xpath("//div[strong[contains(.,'Zona')]]/parent::td/following-sibling::td//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        rent = response.xpath("//font[strong[contains(.,'Prezzo')]]/parent::div/parent::td/following-sibling::td//text()").get()
        if rent:
            rent = rent.split("€")[1].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        desc = "".join(response.xpath("//div[@class='immobiliannuncio']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images = [f"http://archivio.studiosumisura.it/easyhome{x}" for x in response.xpath("//div[@class='galleryimmobili']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Studio Su Misura")
        item_loader.add_value("landlord_phone", "(+39) 055 213085")
        item_loader.add_value("landlord_email", "studiosumisura@studiosumisura.it")
        
        yield item_loader.load_item()