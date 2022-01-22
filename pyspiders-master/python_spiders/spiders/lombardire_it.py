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
    name = 'lombardire_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Lombardire_PySpider_italy"
    start_urls = ['https://lombardire.it/immobili/residenziale-milano?contratto=affitto&nome=&tipologia=&prezzo_da=&prezzo_a=&metri_da=&metri_a=&camere=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[@class='inner-box']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://lombardire.it/immobili/residenziale-milano?nome=&tipologia=&prezzo_da=&prezzo_a=&metri_da=&metri_a=&camere=&contratto=affitto&pagina={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[strong[contains(.,'Tipologia')]]/following-sibling::div[1]/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[@class='auto-container']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//div[@class='inner-column']/p/small/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())
        
        rent = response.xpath("//div[@class='re-table-cell price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('€')[0])
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("(//div[@class='re-table-cell']/text()[contains(.,'mq')])[1]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('mq')[0])
        
        city = response.xpath("//strong[contains(.,'Provincia')]//parent::div/following-sibling::div[1]/text()").get()
        if city:
            item_loader.add_value("city", city)
        address = response.xpath("(//strong[contains(.,'Zona')]//parent::div/following-sibling::div[1]/text())[1]").get()
        if address:
            item_loader.add_value("address", address + ", " + city)
        room_count = response.xpath("//strong[.='Locali']//parent::div/following-sibling::div[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//strong[contains(.,'Bagni')]//parent::div/following-sibling::div[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = "".join(response.xpath("//div[@class='text']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [x for x in response.xpath("//div[@class='image-box']/div/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        utilities = response.xpath("//strong[contains(.,'Spese Condominio')]//parent::div/following-sibling::div[1]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0])
        
        energy_label = response.xpath("//div[@class='text-right font-size-16'][contains(.,'energetica')]/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        item_loader.add_value("landlord_name", "LOMBARDI REAL ESTATE")
        landlord_email = response.xpath("//ul[@class='contact-list clearfix']/li/i[contains(@class,'envelope')]/parent::li/a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        landlord_phone = response.xpath("//ul[@class='contact-list clearfix']/li/i[contains(@class,'phone')]/parent::li/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("mansarda" in p_type_string.lower() or "trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None