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
    name = 'affitto_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = 'Affitto_PySpider_italy'
    # headers = {
    #     ":authority": "www.affitto.it",
    #     ":method": "GET",
    #     ":path": "/elenco.php?comune_autocomplete=Milano+e+Provincia&comune=Milano&zona=&tipologia=appartamento&locali_min=&locali_max=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&arredamento=&giardino=&numservizi=&postoauto=&codice_tipo_utente=&testo=&isProvincia=isProvincia&codice_tipo_annuncio=OI&ricerca_avanzata=1",
    #     ":scheme": "https",
    #     "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    #     "accept-encoding": "gzip, deflate, br",
    #     "accept-language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
    #     "cache-control": "max-age=0", 
    #     "sec-ch-ua": '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
    #     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    # }
    # 1. FOLLOWING
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://www.affitto.it/"],
            }           

        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(
                            url=item,
                            callback=self.jump,
                            #headers=self.headers,
                        )

    def jump(self, response):
        
        for item in response.xpath("(//div[@class='col-xs-3'])[2]/div/a"):
            url = item.xpath("./@href").get()
            yield Request(url, callback=self.parse)
        
        next_city = response.xpath("(//div[@class='col-xs-3'])[2]/div/following-sibling::div/a/@href").get()  
        if next_city:           
            url = response.urljoin(next_city)
            yield Request(url, callback=self.jump)

    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        
        for item in response.xpath("//div[@class='col-xs-12']/h2//a[@class='link_dettaglio']"):
            url = item.xpath("./@href").get()
            yield Request(url, callback=self.populate_item)
            seen = True       
        
        if page == 2 or seen:
            url = response.urljoin(f"?pg={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        studio_check = response.xpath("//p[@class='text-center']/i[@class='fa fa-cubes fa-2x']/parent::p/text()").get()
        if studio_check and "monolocale" in studio_check.lower():
            item_loader.add_value("property_type", "studio")

        property_type = response.xpath("//div[@class='col-xs-12 col-sm-8 col-xs-offset-0 col-65']/h2/text()").get()
        if get_p_type_string(property_type):        
            item_loader.add_value("property_type", get_p_type_string(property_type))
        elif get_p_type_string(property_type):
            property_type = response.xpath("//div[@class='col-xs-12 col-sm-8 col-xs-offset-0 col-65']/h1/text()").get()
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        
        title = response.xpath("//div[@class='col-xs-12 col-sm-8 col-xs-offset-0 col-65']/h2/text()").get()
        if title:
            item_loader.add_value("title", title)
            
        external_id = response.xpath("//div[@class='carr_dettagli']/p/strong[contains(.,'Codice annuncio:')]/parent::p/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        room_count = response.xpath("//p[@class='text-center']/i[@class='fa fa-bed fa-2x']/parent::p/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(' ')[0].strip())
     
        price = response.xpath("//p[@class='pull-right prezzo']/text()[contains(.,'€')]").extract()
        if price:
            rent = price[0] 
            item_loader.add_value("rent", rent.split('€')[0].strip().replace(".",""))
        ut = response.xpath("//p[@class='pull-right prezzo']/span/text()[contains(.,'€')]").extract()
        if ut:
            utilities = ut[0] 
            item_loader.add_value("utilities", utilities.split('€')[0].strip().replace(".",""))
        item_loader.add_value("currency", "EUR")
        
        city = response.xpath("//div[@class='col-xs-12 col-sm-8 col-xs-offset-0 col-65']/h1/text()").get()
        if city:
            item_loader.add_value("city", city.split(' a ')[-1].split(' ')[0])

        address = response.xpath("//div[@class='col-xs-12 col-sm-8 col-xs-offset-0 col-65']/p/strong[contains(.,'Indirizzo')]/parent::p/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        else:
            address = response.xpath("//p[@class='zona_title']/text()[1]").get()
            if address:
                item_loader.add_value("address", address.strip())
            else:
                if city:
                    item_loader.add_value("address", city.split(' a ')[-1].split(' ')[0])


        bathroom= response.xpath("//p[@class='text-center']/i[@class='properticons properticons-baths']/parent::p/text()").get()
        if bathroom:
            bathroom = bathroom.strip()
            if bathroom and "un" in bathroom.lower():
                item_loader.add_value("bathroom_count", "1")
            if bathroom and "due" in bathroom.lower():
                item_loader.add_value("bathroom_count", "2")
            if bathroom and "tre" in bathroom.lower():
                item_loader.add_value("bathroom_count", "3")
            if bathroom and "quattro" in bathroom.lower():
                item_loader.add_value("bathroom_count", "4")
        square_meters = response.xpath("//p[@class='text-center']/i[@class='fa fa-home fa-2x']/parent::p/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('Mq')[0].strip())
        
        energy_label = response.xpath("//div[@class='carr_dettagli']/p/strong[contains(.,'energetica')]/parent::p/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.replace(".","").replace(":","").strip().split('-')[-1].strip())
        
        furnished = response.xpath("//div[@class='carr_dettagli']/p/strong[contains(.,'Arredamento')]/parent::p/text()").get()
        if furnished and "Arredato" in furnished:
            item_loader.add_value("furnished", True)
        parking = response.xpath("//div[@class='carr_dettagli']/p/strong[contains(.,'Garage ')]/parent::p/text()").get()
        if parking and "Posto scoperto" in parking:
            item_loader.add_value("parking", True)
        balcony = response.xpath("//ul[@class='servizi_list']/li/i[@class='fa fa-check-square']/parent::li/text()[contains(.,'Balconi')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        desc = "".join(response.xpath("//div[@id='google_translate_element']//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
        latlng = response.xpath("(//a[@rel='nofollow'])[1]/@href").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('?q=')[-1].split(',')[0])
            item_loader.add_value("longitude", latlng.split('?q=')[-1].split(',')[-1])
        
        images = [x for x in response.xpath("//div[@class='royalSliderThumbs royalSlider rsDefault']/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        floor_plan_images = [x for x in response.xpath("(//div[@class='modal fade planimetrie-modal']/div/div//img/@src)[1]").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        landlord_name=response.xpath("//div[@class='text-center']/p/strong/text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            landlord_name=response.xpath("//div[@class='text-center']/p/strong/span/text()").extract_first()
            item_loader.add_value("landlord_name", landlord_name)
        landlord_phone=response.xpath("(//div[@class='text-center']/p[@class='phone_full_btn']/big/span[@class='phone_full hide']/text())[1]").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            landlord_phone=response.xpath("(//p[@class='phone_full_btn']/span[@class='phone_full hide']/text())[2]").extract_first()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone)
            else:
                landlord_phone="".join(response.xpath("//span[@class='phone_full']/text()").extract())
                if landlord_phone:
                    item_loader.add_value("landlord_phone", landlord_phone.strip())
            
        # landlord_email=response.xpath("//div[@class='info-box']//a[@class='mail']//text()").extract_first()
        # if landlord_email:
        #     item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartamento" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("attico" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("loft/open space" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa indipendente" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower()):
        return "house"
    else:
        return None