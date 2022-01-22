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
import dateparser

class MySpider(Spider):
    name = 'sequoia_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.sequoia-immobilier.fr/advanced-search-3/?filter_search_action%5B%5D=a-louer&filter_search_type%5B%5D=appartement&advanced_city=&advanced_area=&advanced_rooms=&advanced_bath=&price_low=&price_max=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.sequoia-immobilier.fr/advanced-search-3/?filter_search_action%5B%5D=a-louer&filter_search_type%5B%5D=maison&advanced_city=&advanced_area=&advanced_rooms=&advanced_bath=&price_low=&price_max=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'item active')]/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//li[@class='roundright']/a/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Sequoia_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div/span[@class='adres_area']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='panel-body']//div[strong[.='Code postal:']]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='panel-body']//div[strong[.='Ville:']]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div/h1[contains(@class,'entry-title entry-prop')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='panel-body']//div[strong[.='ID:']]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[@class='panel-body']//div[strong[.='Etage:']]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='property_description']/p//text()", input_type="M_XPATH", replace_list={"\xa0":""})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='panel-body']//div[strong[.='Surface:']]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='panel-body']//div[strong[.='Chambres:']]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='panel-body']//div[strong[.='Salles de bains:']]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent_string", input_value="//span[@class='price_area']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='panel-body']//div[strong[.='Dépôt De Garantie:']]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//div[@class='panel-body']//div[strong[.='Charges Mensuelles:']]/text()[.!=' 0 €']", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[@id='googleMap_shortcode']/@data-cur_lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[@id='googleMap_shortcode']/@data-cur_long", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//h1[@class='title_agent_slider']//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='agent_meta_slider']/span[.='Phone:']/following-sibling::text()[1]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[@class='agent_meta_slider']/span[.='Adresse de messagerie:']/following-sibling::text()[1]", input_type="F_XPATH")
        images = []
        img = response.xpath("//div[@id='owl-demo']/div[@class='item']/@style").extract()
        if img:
            for j in img:
                img = j.split("background-image:url(")[1].split(")")[0]
                images.append(img)
            item_loader.add_value("images", images)

        available_date = response.xpath("//div[@class='panel-body']//div[strong[.='Disponibilité:']]/text()").extract_first() 
        if not available_date:
            available_date = response.xpath("substring-after(//div[@id='property_description']/p//text()[contains(.,'Disponible à partir du')],'Disponible à partir du')").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        parking =response.xpath("//div[@class='panel-body']//div[strong[.='Parking:']]/text()").extract_first()    
        if parking:
            if "non" in parking.lower() or parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        terrace =response.xpath("//div[@class='panel-body']//div[strong[.='Terrasse:']]/text()").extract_first()    
        if terrace:
            if "non" in terrace.lower() or terrace.strip() == "0":
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        elevator =response.xpath("//div[@class='panel-body']//div[strong[.='Ascenseur:']]/text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        energy_label = response.xpath("//script[contains(.,'dpeges.dpe') and contains(.,'value:')]/text()").extract_first()    
        if energy_label:
            energy = energy_label.split("value:")[1].split(",")[0].strip()
            if energy.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy))
        furnished =response.xpath("//div[i[contains(@class,'icon-check')]]/text()[.='Meublé']").extract_first()    
        if furnished:
            item_loader.add_value("furnished", True)
    
        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
