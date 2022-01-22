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
    name = 'imvaloris_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.imvaloris.fr/nos-biens/?egap=1&rubrique2=Location%20&categorie2_a=Appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.imvaloris.fr/nos-biens/?egap=1&rubrique2=Location&categorie2_a=Maison",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 2)
        max_pages = response.xpath("//div[@class='pagination']/a[last()]/text()").get()

        for item in response.xpath("//a[@class='btnplus']/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if max_pages:
            if page <= int(max_pages):
                follow_url = response.url.replace("?egap=" + str(page - 1), "?egap=" + str(page))
                yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Imvaloris_PySpider_france")      
        item_loader.add_xpath("title", "//div[@class='contentannoncedetail']//h2/text()")
        item_loader.add_xpath("external_id", "substring-after(//div[@class='contentannoncedetail']//h4/text(),'Ref. ')")
        room_count = response.xpath("//div[@class='contentannoncedetail']//h3/text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("pièce")[0])
    
        address = response.xpath("//div[@class='contentannoncedetail']//h3/font/font/text()").get()
        if address:
            address = address.split("/")[0]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.strip().split(" ")[0])
            item_loader.add_value("city", " ".join(address.strip().split(" ")[1:]))
        
        square_meters = response.xpath("//div[@class='contentannoncedetail']//h3/font/text()[contains(.,' m')]").get()
        if square_meters:
            square_meters = square_meters.split(" m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
      
        description = " ".join(response.xpath("//div[contains(@class,'pavedescription')]/p//text()[.!='>> Consultez nos tarifs']").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//div[@class='gallery-slider__images']/div/div//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        rent = "".join(response.xpath("//p[@class='prix']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        deposit = response.xpath("//p[@class='charge']//text()[contains(.,'Dépot de garantie')]/following-sibling::b[1]//text()").get()
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.split("€")[0])))
        utilities = response.xpath("//p[@class='charge']//text()[contains(.,'Charges ')]/following-sibling::b[1]//text()").get()
        if utilities:
            item_loader.add_value("utilities", int(float(utilities.split("€")[0])))
        item_loader.add_value("landlord_name", "IM VALORIS")
        item_loader.add_value("landlord_phone", "02 47 200 000")
        item_loader.add_value("landlord_email", "contact@imvaloris.com")
        yield item_loader.load_item()