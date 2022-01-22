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
    name = 'gestiona_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.gestiona-immo.com/recherche-avancee/page/1/?adv_location&filter_search_action%5B0%5D&submit=RECHERCHER&is10=10&filter_search_type%5B0%5D=appartement&advanced_city&surface-min&pieces-min&chambres-min&price_low=100&price_max=10000000", 
                "property_type": "apartment"
            },
            {
                "url": "https://www.gestiona-immo.com/recherche-avancee/?adv_location=&filter_search_action%5B%5D=&submit=RECHERCHER&is10=10&filter_search_type%5B%5D=maison&advanced_city=&surface-min=&pieces-min=&chambres-min=&price_low=100&price_max=10000000", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_ajax_container']//a[contains(.,'En savoir plus')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        next_button = response.xpath("//li[@class='roundright']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        vente = response.xpath("//div[@class='notice_area']//a/text()[contains(.,'Vente')]").extract_first()
        if vente:
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("floor", "normalize-space(//div[@class='panel-body']/div/strong[contains(.,'Etage')]/following-sibling::text())")
        item_loader.add_xpath("bathroom_count", "normalize-space(//div[@class='panel-body']/div/strong[contains(.,'Salle de bains')]/following-sibling::text())")
        item_loader.add_xpath("utilities", "normalize-space(//div[@class='panel-body']/div/strong[contains(.,'Charges')]/following-sibling::text())")
  
        item_loader.add_value("external_source", "Gestiona_Immo_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")

        prop =  " ".join(response.xpath("//div[@class='property_categs']/a/text()").extract())
        if prop:
            if "appartement" in prop.lower():             
                item_loader.add_value("property_type", "apartment")
            else:
                item_loader.add_value("property_type", response.meta.get('property_type'))

        desc="".join(response.xpath("//div[@class='wpestate_property_description']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        images=[x for x in response.xpath("///div[@class='carousel-inner']/div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        city="".join(response.xpath("//div[@class='panel-body']/div/strong[.='Pays:']/following-sibling::text()").getall())
        if city:
            item_loader.add_value("city", city.strip())

        address=" ".join(response.xpath("//div[@class='panel-body']/div/strong[.='Ville:']/following-sibling::a/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        zipcode="".join(response.xpath("//div[@class='panel-body']/div/strong[.='Zip:']/following-sibling::text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        external_id="".join(response.xpath("//div[@class='panel-body']/div/strong[.='Référence:']/following-sibling::text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        room_count="".join(response.xpath("//div[@class='panel-body']/div/strong[.='Chambres:']/following-sibling::text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count="".join(response.xpath("//div[@class='panel-body']/div/strong[.='Pièces:']/following-sibling::text()").getall())
            if room_count:
                item_loader.add_value("room_count", room_count.strip())


        rent="".join(response.xpath("//div[@class='panel-body']/div/strong[.='Prix:']/following-sibling::text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(".","").replace(" ","").strip())

        meters="".join(response.xpath("normalize-space(//div[@class='panel-body']/div/strong[contains(.,'Surface')]/following-sibling::text())").getall())
        if meters:
            item_loader.add_value("square_meters", meters.strip().split(" "[0]))


        parking = "".join(response.xpath("//div[@class='panel-body']/div/strong[.='Parking:']/following-sibling::text()").extract())
        if parking:
            if parking.strip()!="0":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        elevator = "".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Ascenseur')]/text()").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = "".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Terrasse')]/text()").extract())
        if terrace:
            item_loader.add_value("terrace", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'markers2')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('markers2":"[[')[1].split(",")[1]
            longitude = latitude_longitude.split('markers2":"[[')[1].split(',')[2].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_xpath("landlord_name", "//div[@class='agent_unit']/div/h4/a/text()")
        landlord_phone = response.xpath("//div[@class='agent_detail']/i[@class='fa fa-mobile']/following-sibling::text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            landlord_phone = response.xpath("//div[@class='agent_detail']/i[@class='fa fa-phone']/following-sibling::text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_xpath("landlord_email", "//div[@class='agent_detail']/i[@class='fa fa-envelope-o']/following-sibling::text()")
        yield item_loader.load_item()