# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'novafoncier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.novafoncier.fr/recherche-biens/page/{}/?type%5B0%5D=appartement-2&type%5B1%5D=duplex&type%5B2%5D=loft-2&status=location",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.novafoncier.fr/recherche-biens/page/{}/?type%5B%5D=maison-2&status=location",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://www.novafoncier.fr/recherche-biens/page/{}/?type%5B%5D=studio&status=location",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='rh_figure_property_list_one']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page + 1, "base":base})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Novafoncier_PySpider_france")
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.split(" – ")[-1].strip())
            item_loader.add_value("city", title.split(" – ")[-1].strip())
        external_id = response.xpath("//p[@class='id']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
   
        bathroom_count = response.xpath("//div[contains(@class,'prop_bathrooms')]/div/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        if response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//div[@class='rh_property__meta prop_bedrooms']/div/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        utilities = response.xpath("//div[@class='rh_content']/p//text()[contains(.,'provisions sur charges')]").get()
        if utilities:
            utilities = utilities.split("provisions sur charges")[0].split("+")[-1].strip()
            item_loader.add_value("utilities", utilities)
        description = "".join(response.xpath("//div[@class='rh_content']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//div[contains(@class,'prop_area')]/div/span[@class='figure']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.strip())))
        rent = response.xpath("//p[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace("\xa0",""))
        
        available_date = response.xpath("//div[@class='rh_content']/p//text()[contains(.,'Disponible au')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponible au")[-1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        lat_lng = response.xpath("//script[contains(.,'lng')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split('"lat":"')[1].split('"')[0].strip())
            item_loader.add_value("longitude", lat_lng.split('"lng":"')[1].split('"')[0].strip())
        energy_label = response.xpath("//script[contains(.,'dpeges.dpe')]/text()").get()
        if energy_label:
            energy = energy_label.split("value:")[-1].split(",")[0].strip()
            if energy.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy))
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-detail-slider-two']//li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
            
        item_loader.add_value("landlord_name", "Nova Foncier")
        item_loader.add_value("landlord_phone", "03 20 311 868")
        item_loader.add_value("landlord_email", "contact@novafoncier.fr")

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