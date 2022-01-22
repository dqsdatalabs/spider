# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'grimaldifranchising_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Grimaldifranchising_PySpider_italy"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.grimaldifranchising.it/immobili/search?areas=&dove=regione-lombardia&ordinamento=&visualizzazione=&agenzia=0&consulente=0&contratto=affitto&uso=abitativo&numerolocali=&prezzominimo=&prezzomassimo=&mqmin=&mqmax=&tipologia=appartamento",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.grimaldifranchising.it/immobili/search?areas=&dove=regione-lombardia&ordinamento=&visualizzazione=&agenzia=0&consulente=0&contratto=affitto&uso=abitativo&numerolocali=&prezzominimo=&prezzomassimo=&mqmin=&mqmax=&tipologia=casa-indipendente",
                    "https://www.grimaldifranchising.it/immobili/search?areas=&dove=regione-lombardia&ordinamento=&visualizzazione=&agenzia=0&consulente=0&contratto=affitto&uso=abitativo&numerolocali=&prezzominimo=&prezzomassimo=&mqmin=&mqmax=&tipologia=villa",
                    "https://www.grimaldifranchising.it/immobili/search?areas=&dove=regione-lombardia&ordinamento=&visualizzazione=&agenzia=0&consulente=0&contratto=affitto&uso=abitativo&numerolocali=&prezzominimo=&prezzomassimo=&mqmin=&mqmax=&tipologia=attico"
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
        
        for item in response.xpath("//div[contains(@class,'serp-card--property')]"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//li[@class='pagination-next']//@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        rent = response.xpath("//span[contains(@class,'price')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace("€","").strip())
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//span[span[contains(@class,'icon-metri')]]/parent::li/span[@class='data']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//span[span[contains(@class,'icon-local')]]/parent::li/span[@class='data']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[span[contains(@class,'icon-bagno')]]/parent::li/span[@class='data']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        address = "".join(response.xpath("//span[contains(@class,'header__address')]/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip().split(" ")[-1])
            item_loader.add_value("zipcode", address.split(",")[-1].strip())

        images = [x for x in response.xpath("//section[@class='section-gallery']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        desc = "".join(response.xpath("//div[@class='description']//p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        utilities = response.xpath("//li[span[contains(.,'Spese condominiali')]]/span[2]/text()").get()
        if utilities:
            utilities = utilities.split("/")[0].split("€")[1].strip()
            item_loader.add_value("utilities", utilities)
        
        energy_label = response.xpath("//li[span[contains(.,'energetica')]]/span[2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        balcony = response.xpath("//li[span[contains(.,'balcon')]]/span[2]/text()[.!='0']").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        item_loader.add_xpath("latitude", "//input[@id='maplat']/@value")
        item_loader.add_xpath("longitude", "//input[@id='maplng']/@value")
        
        landlord_name = response.xpath("//h2[@class='info__title']//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Grimaldi Franchising Spa")
        
        landlord_phone = response.xpath("substring-after(//span[contains(@class,'icon-phone')]/following-sibling::a/@href,':')").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "02/89046900")
            
        landlord_email = response.xpath("substring-after(//span[contains(@class,'icon-mail')]/following-sibling::a/@href,':')").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", "info@grimaldifranchising.it")
        
        yield item_loader.load_item()