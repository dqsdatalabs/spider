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
    name = 'tft_net'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Tft_PySpider_italy"
    start_urls = ['https://www.tft.net/wp-admin/admin-ajax.php?action=lista_immobili_trovati&search_lang_principal=eng&search_categoria=&search_codice=&zone_selezionate=&page_id=624&search_tipologia=&search_prezzo_range=0%7C60000&search_superficie_range=0%7C10000&search_stanze_range=0%7C10&search_bagni_range=0%7C10&search_contratto=Affitto&search_portiere=&search_ascensore=&search_terrazze=&search_balconi=&search_condizionatore=&search_arredato=&search_piscina=&search_chk_box=&search_posto_auto=&search_areddito=&search_giardino=&search_riscaldamento=&search_stato=&search_piano=&posizioni_mappa=&map_position_center=&order=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='immagine']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.tft.net/wp-admin/admin-ajax.php?action=lista_immobili_trovati&search_lang_principal=eng&search_categoria=&search_codice=&zone_selezionate=&page_id=624&search_tipologia=&search_prezzo_range=0%7C60000&search_superficie_range=0%7C10000&search_stanze_range=0%7C10&search_bagni_range=0%7C10&search_contratto=Affitto&search_portiere=&search_ascensore=&search_terrazze=&search_balconi=&search_condizionatore=&search_arredato=&search_piscina=&search_chk_box=&search_posto_auto=&search_areddito=&search_giardino=&search_riscaldamento=&search_stato=&search_piano=&search_page={page}&posizioni_mappa=&map_position_center=&order="
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[@class='titolo' and contains(.,'Type')]/following-sibling::div/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//div[@class='codice']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("#")[1])

        title = response.xpath(
            "//div[@class='tipo_comune']//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = response.xpath(
            "//div[@class='box_posizione_immobile']//div[contains(.,'Location')]//following-sibling::div//text()").get()
        if city:
            city=city.split(",")[1:2]
            item_loader.add_value("city", city)

        address = response.xpath(
            "//div[@class='box_posizione_immobile']//div[contains(.,'Location')]//following-sibling::div//text()").get()
        if address:
            item_loader.add_value("address", address)

        description = response.xpath(
            "//div[@class='box_descrizione_immobile']//div[contains(@class,'descrizione-testo')]//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//div[@class='info_immobile prima-riga']//div[contains(.,'Price')]//following-sibling::div//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//div[@class='info_immobile seconda-riga']//div[contains(.,'Sq')]//following-sibling::div//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])

        bathroom_count = response.xpath(
            "//div[@class='info_immobile ultimo-prima-riga']//div[contains(.,'Bathrooms:')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//div[@class='info_immobile prima-riga']//div[contains(.,'Rooms:')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        energy_label = response.xpath(
            "//div[@class='riga_tabella']//div[contains(.,'Energetic Class')]//following-sibling::div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@id='masterslider_dettaglio']//div[contains(@class,'ms-slide')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [response.urljoin(x) for x in response.xpath(
            "//div[@id='masterslider_planimetrie']//div[contains(@class,'ms-slide')]//img//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)


        furnished = response.xpath(
            "//div[@class='features']//following-sibling::div//text()[contains(..,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath(
            "//div[@class='features']//following-sibling::div//text()[contains(..,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')][1]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("LatLng(")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        item_loader.add_value("landlord_name", "TFT BUILDING MANAGEMENT SRL")
        item_loader.add_value("landlord_phone", "39 06 3297036")
        item_loader.add_value(
            "landlord_email", "segreteria@tft.net")

        yield item_loader.load_item()
                
def get_p_type_string(p_type_string):
    if p_type_string and ("apartment" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "trilocale" in p_type_string.lower() or "house" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None