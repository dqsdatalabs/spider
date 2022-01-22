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
    name = 'immocity_com'
    execution_type='testing' 
    country='france'
    locale='fr'
    external_source = "CabinetGerasco_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immocity.com/index.php?contr=biens_liste&tri_lots=date&type_transaction=1&localisation=&hidden-localisation=&type_lot%5B%5D=appartement&surface=&nb_piece=0&nb_chambre=0&budget_min=&budget_max=&page=0&vendus=0&submit_search_1=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immocity.com/index.php?contr=biens_liste&tri_lots=date&type_transaction=1&localisation=&hidden-localisation=&type_lot%5B%5D=maison&surface=&nb_piece=0&nb_chambre=0&budget_min=&budget_max=&page=0&vendus=0&submit_search_1="
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
        for item in response.xpath("//div[contains(@class,'properties-grid')]/div/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            address = item.xpath("./..//div[h4]/text()[3]").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"address":address})
        
        next_page = response.xpath("//a[contains(.,'Suivante >')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")


        item_loader.add_xpath("external_id", "normalize-space(substring-after(//div/text()[contains(.,'REF. ')],'REF. '))")
        room_count = response.xpath("substring-before(//div[@class='pictosCriteresDiv']/text()[contains(.,'pièces')],'pièces')").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        studio = response.xpath("//div[@class='pictosCriteresDiv'][img[contains(@src,'baig') or contains(@src,'plan')]]/text()[.!='Non']").get()
        if studio and "studio" in studio:
            item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count", "//div[@class='pictosCriteresDiv'][img[contains(@src,'baig') or contains(@src,'douche')]]/text()[.!='Non']")
    
        rent = response.xpath("//span[@class='TitrePrix']/text()").get()
        if rent:
            rent=rent.split(".")[0]
            item_loader.add_value("rent", rent)       

        item_loader.add_value("currency", "EUR")
        address = response.meta.get('address')
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            if address.count(" ")>0:
                item_loader.add_value("zipcode", address.split(" ")[-1])
                item_loader.add_value("city"," ".join(address.split(" ")[:-1]))
            else:
                item_loader.add_value("zipcode", address)
        adrescheck=item_loader.get_output_value("address")
        if not adrescheck:
            adres=response.xpath("//h1/text()[2]").getall()
            if adres:
                item_loader.add_value("address",adres)
            zipcheck=item_loader.get_output_value("zipcode")
            if zipcheck=="":
                adres1=item_loader.get_output_value("address")
                item_loader.add_value("zipcode",adres1.split(" ")[0])
                item_loader.add_value("city",adres1.split(" ")[1])
                
      
        deposit = response.xpath("substring-after(//div//text()[contains(.,'de garantie :')],'de garantie :')").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(",")[0].replace(" ","").strip())
        depositcheck=item_loader.get_output_value("deposit")
        if not depositcheck:
            deposit=" ".join(response.xpath("//div//text()[contains(.,'de garantie')]").extract())
            if deposit:
                deposit=deposit.split("tie:")[1].split("€")[0]
                if deposit:
                    item_loader.add_value("deposit",deposit)
        utilities = response.xpath("substring-after(//div//text()[contains(.,'Charges provision')],':')").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0].replace(" ","").strip())
        floor = response.xpath("//li//text()[contains(.,'Floor')]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[-1].strip())

        square = response.xpath("//div[@class='pictosCriteresDiv'][img[contains(@src,'metre')]]/text()").extract_first()
        if square:       
            square_title=square.split("m")[0].strip()
            item_loader.add_value("square_meters", str(int(float(square_title))))

        elevator = response.xpath("//div[@class='pictosCriteresDiv'][img[contains(@src,'ascenseur')]]").get()
        if elevator:
                item_loader.add_value("elevator",True)
        balcony = response.xpath("//div[@class='pictosCriteresDiv'][img[contains(@src,'balcon')]]").get()
        if balcony:
                item_loader.add_value("balcony",True)
        floor = response.xpath("//div[@class='pictosCriteresDiv'][img[contains(@src,'immeuble')]]/text()").get()
        if floor:
                item_loader.add_value("floor",floor.split("/")[0])
    

        desc = "".join(response.xpath("//div[@class='col-sm-12']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        

        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//a//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("landlord_name", "//div[@class='col-md-12 col-xs-12']//strong/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='col-md-12 col-xs-12']//input[@id='email_destinataire']/@value")
        item_loader.add_value("landlord_phone","01.48.90.44.90")

        yield item_loader.load_item()