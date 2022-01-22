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
    name = 'agenziarepetto_net'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Agenziarepetto_PySpider_italy"
    start_urls = ['http://agenziarepetto.net/immobili.asp?go=1&ag=&r=&p=&v=&chm=&mq=&pz=&rif=&zn=&vt=&plan=&ft=&w=&pchiave=&pr=0&ct=0&c=0&t=2&l=0&Ricerca=ricerca']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Dettaglio')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("id=")[-1])
        rent=response.xpath("//div[.='Prezzo:']/following-sibling::div/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(",")[0])
        item_loader.add_value("currency","EUR")
        city=response.xpath("//div[@class='sw_dati']/h2/strong/text()").get()
        if city:
            item_loader.add_value("city",city.split("(")[0])
        address=item_loader.get_output_value("city")
        address2=response.xpath("//div[@class='sw_dati']/h2/text()").get()
        if address2:
            address2=address2.split("Via")[-1]
        item_loader.add_value("address",address+address2)
        square_meters=response.xpath("//div[.='Mq:']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(",")[0])

        room_count=response.xpath("//div[.='Numero camere:']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            room_count2=response.xpath("//div[.='Numero locali:']/following-sibling::div/text()").get()
            if room_count2:
                item_loader.add_value("room_count",room_count2.split(" ")[0])
        bathroom_count=response.xpath("//div[.='Numero bagni:']/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        utilities=response.xpath("//div[.='Spese condominiali:']/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities)
        elevator=response.xpath("//div[.='Ascensori:']/following-sibling::div/text()").get()
        if elevator and not "no" in elevator.lower():
            item_loader.add_value("elevator",True)
        balcony=response.xpath("//div[.='Numero balconi:']/following-sibling::div/text()").get()
        if balcony and not "no" in balcony.lower():
            item_loader.add_value("balcony",True)
        energy_label=response.xpath("//strong[.='Classe Energetica']/parent::div/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("(")[0])
        furnished=response.xpath("//div[.='Arredato:']/following-sibling::div/text()").get()
        if furnished and furnished=="Si":
            item_loader.add_value("furnished",True)
        desc=" ".join(response.xpath("//div[@class='sw_dati']//h2//text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//form[@name='mygallery']/select/option/@value").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name"," IMMOBILIARE REPETTO")
        item_loader.add_value("landlord_phone","010593143")
        item_loader.add_value("landlord_email","agenziarepetto@libero.it")
        


        yield item_loader.load_item()