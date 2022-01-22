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
    name = 'barnoud_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Barnoud_Immobilier_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.barnoud-immobilier.fr/location/appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.barnoud-immobilier.fr/location/maison",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//article[contains(@class,'node-')]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'suivant')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        check_page = response.xpath("//div[@class='empty']/p/text()[contains(.,'aucun résultat')]").get()
        if check_page:
            return
           
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)      
        item_loader.add_xpath("title", "//h1/span[1]/text()")
        item_loader.add_xpath("external_id", "substring-after(//div[@class='ref']/text(),'réf. ')")
        room_count = response.xpath("//ul/li[contains(.,'Nb. de chambres :')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1])
        else:
            room_count = response.xpath("//ul/li[contains(.,'Nb. de pièces : ')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(":")[1])
        bathroom_count = response.xpath("//ul/li[contains(.,'Nb. de salles de bain : ')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1])
        address = response.xpath("//div[@class='subtitle']/text()").get()
        if address:
            item_loader.add_value("address", address)
        item_loader.add_xpath("zipcode", "substring-after(//ul/li[contains(.,'Code postal : ')]/text(),': ')")
        item_loader.add_xpath("city", "substring-after(//ul/li[contains(.,'Ville : ')]/text(),': ')")
        item_loader.add_xpath("utilities", "substring-after(//ul/li[contains(.,'Charges : ')]/text(),': ')")
        item_loader.add_xpath("floor", "substring-after(//ul/li[contains(.,'Étage : ')]/text(),': ')")
        item_loader.add_xpath("energy_label", "substring-after(//div/img[contains(@alt,'DPE - ')]/@alt,'- ')")        
        square_meters = response.xpath("//ul/li[contains(.,'Surface :')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" m")[0].split(":")[1].strip())
      
        description = " ".join(response.xpath("//div[@id='description']//p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        furnished = response.xpath("substring-after(//ul/li[contains(.,'Meublé :')]/text(),': ')").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator = response.xpath("substring-after(//ul/li[contains(.,'Ascenseur : ')]/text(),': ')").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        balcony = response.xpath("substring-after(//ul/li[contains(.,'Balcon : ')]/text(),': ')").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        terrace = response.xpath("substring-after(//ul/li[contains(.,'Terrasse : ')]/text(),': ')").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        parking = response.xpath("//ul/li[contains(.,'Nb. de garages : ') or contains(.,'Nb. de parkings :')]/text()[not(contains(.,'Aucun'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        images = [x for x in response.xpath("//div[contains(@class,'gallery-thumbs')]//div[@class='swiper-wrapper']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
        rent = response.xpath("//ul/li[contains(.,'Prix : ')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        landlord_name = " ".join(response.xpath("//div[contains(@class,'immobilier-contact-col')]//h3/strong/text()").getall()) 
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'immobilier-contact-col')]//p[@class='phone m-0']/span/text()")
        item_loader.add_value("landlord_email", "barnoud.location@orange.fr")
        
        yield item_loader.load_item()