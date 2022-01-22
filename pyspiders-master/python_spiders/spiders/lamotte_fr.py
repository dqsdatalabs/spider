# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser


class MySpider(Spider):
    name = 'lamotte_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.'
    external_source = "Lamottegestion_PySpider_france_fr"
    custom_settings = {
        "PROXY_ON": True
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.lamotte.fr/annonces/?acquisition=Location&immobilier=Logement&type%5B%5D=Appartement",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.lamotte.fr/annonces/?acquisition=Location&immobilier=Logement&type%5B%5D=Maison",
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
        for item in response.xpath("//div[@id='result']/div"):
            url = item.xpath("./a/@href").get()
            address = item.xpath(".//h2/span/text()").get()
            sqm = item.xpath(".//div[@class='programme__city']/text()").get()
            room = item.xpath(".//div[@class='programme__size']/text()").get()
            yield Request(url, 
                        callback=self.populate_item,
                        meta={
                            "property_type": response.meta.get('property_type'),
                            "address":address,
                            "sqm":sqm,
                            "room":room,                        
                        })
        
        next_page = response.xpath("//li[@class='pagination--active']/following-sibling::li[1]/a/@href").get()   
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        room_count = response.meta.get('room')
        sqm = response.meta.get('sqm')
        address = response.meta.get('address')

        if room_count and "studio" in room_count:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title =" ".join(response.xpath("//h1/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        item_loader.add_xpath("external_id", "//p[@class='tva']/text()")

        rent= response.xpath("//p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ","").replace("\xa0",""))

        item_loader.add_xpath("utilities", "//p[text()[contains(.,'Charges :')]]/span/text()")
        deposit = response.xpath("//p[text()[contains(.,'garantie :')]]/span/text()").get()
        if deposit:
            deposit = deposit.replace("\xa0","").split("€")[0].strip()
            item_loader.add_value("deposit", int(float(deposit)))

        available_date=response.xpath("//p/text()[contains(.,'Disponibilit')]").get()
        if available_date:
            item_loader.add_value("available_date", available_date.split("Disponibilit"))

        if sqm:
            item_loader.add_value("square_meters", sqm.split('m')[0].strip())
        
        if address:
            item_loader.add_value("address", address.strip())
        
        zipcode = response.xpath("//script[contains(.,'produitLocalisationVilleCode\"')]/text()").get()
        if zipcode:
            zipcode = zipcode.split('produitLocalisationVilleCode":"')[1].split('"')[0]
            item_loader.add_value("zipcode", zipcode)
        
        if room_count and "studio" in room_count:
            item_loader.add_value("room_count", "1")
        elif response.xpath("//li[contains(.,'chambre')]/text()").get():
            item_loader.add_value("room_count",response.xpath("//li[contains(.,'chambre')]/text()").get().split("ch")[0])
        elif room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])

        city = response.xpath("//p[@class='programme__location']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        floor = response.xpath("//li[contains(.,'étage')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip().split(" ")[0])

        energy_label = response.xpath("//ul[@class='score-energie']/li[@class='active']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        bathroom_count= response.xpath("//li[contains(.,'salle d')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("salle d")[0].strip())

        desc=" ".join(response.xpath("//div[@class='col-lg-8 px-lg-0']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[@class='image__wrapper']/a//img/@src[not(contains(.,'data:image/svg+xml,'))]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_xpath("landlord_name","//p[@class='agence__name']/text()")
        item_loader.add_xpath("landlord_phone","//div[@class='agence__phone']/a[@id='texttocopy']/text()")
        # item_loader.add_value("landlord_email","contact@promovente.com")
        
        parking = response.xpath("//li[contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)

        furnished = response.xpath("//li[contains(.,'Meubl')]/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)

        yield item_loader.load_item()