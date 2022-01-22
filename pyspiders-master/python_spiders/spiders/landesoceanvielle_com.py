# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import math

class MySpider(Spider):
    name = 'landesoceanvielle_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.landes-ocean-vielle.com/catalog/advanced_search_result.php?action=update_search&search_id=1680892106902994&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C3%2C17&C_27_tmp=2%2C3%2C17&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&keywords=", "property_type": "house"},
            {"url": "http://www.landes-ocean-vielle.com/catalog/advanced_search_result.php?action=update_search&search_id=1680892106902994&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&keywords=", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})


    def parse(self, response):
        for item in response.xpath("//h2[@class='titre_annonce']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Landesoceanvielle_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//div[@id='content_intro_header']//h1/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        external_id="".join(response.xpath("//div[@class='col-sm-12']/ul/li/span/text()[contains(.,'Référence')]").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        item_loader.add_value("property_type", response.meta.get('property_type'))

        room_count="".join(response.xpath("//div[@class='col-sm-12']/ul/li/span/text()[contains(.,'Chambres')]").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        
        bathroom_count="".join(response.xpath("//div[contains(.,'Salle') and @class='col-sm-6']/following-sibling::*//text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters="".join(response.xpath("//div[@class='col-sm-12']/ul/li/span/text()[contains(.,'Surface')]").extract())
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m²")[0].strip()
            square_meters = math.ceil(float(square_meters))
            item_loader.add_value("square_meters", str(square_meters))

        price="".join(response.xpath("//div[@class='formatted_price_alur2_div']//text()[contains(.,'Loyer')]").extract())
        if price:
            item_loader.add_value("rent_string", price.replace(" ","").strip())
        # item_loader.add_value("currency","EUR")

        utilities = response.xpath("//div[contains(.,'Provision sur charges') and @class='col-sm-6']/following-sibling::*//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[0].strip())
        
        coordinat = response.xpath("//script//text()[contains(.,'maps.LatLng')]").extract_first() 
        if coordinat:
            try:
                map_coor = coordinat.split('google.maps.LatLng(')[1].split(");")[0]
                lat = map_coor.split(",")[0].strip()
                lng = map_coor.split(",")[1].strip()
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
            except:
                pass
        item_loader.add_xpath("energy_label", "//div[div[ .='Conso Energ']]/div/b/text()[. !='Vierge']")
        item_loader.add_xpath("city", "//div[div[ .='Ville']]/div/b/text()")

        item_loader.add_xpath("address", "//div[div[ .='Ville']]/div/b/text()")
        zipcode = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(" ")[0].strip())
        item_loader.add_xpath("floor", "//div[div[ .='Nombre étages']]/div/b/text()")

        desc = "".join(response.xpath("//div[contains(@class,'content_details_description')]/p//text()").extract())
        item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//div[div[ .='Disponibilité']]/div/b/text()[. !='Non']").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        else:
            ava_date = "".join(response.xpath("//div[contains(@class,'content_details_description')]/p//text()[contains(.,'Disponible à partir du')]").extract())
            if ava_date:
                ava_date2 = desc.split("Disponible à partir du")[1].split("**")[0]
                date_parsed = dateparser.parse(ava_date2, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [response.urljoin(x)for x in response.xpath("//ul[@class='slides']/li/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        
        terrace = "".join(response.xpath("//div[div[ contains(.,'parking')]]/div/b/text()").extract()).strip()
        if terrace and terrace != "0":
            item_loader.add_value("parking", True)
        elif terrace:
            item_loader.add_value("parking", False)

        terrace = "".join(response.xpath("//div[div[ .='Ascenseur']]/div/b/text()").extract()).strip()
        if terrace and terrace != "Non":
            item_loader.add_value("elevator", True)
        elif terrace:
            item_loader.add_value("elevator", False)

        terrace = "".join(response.xpath("//div[div[ contains(.,'terrasses')]]/div/b/text()").extract()).strip()
        if terrace and terrace != "0":
            item_loader.add_value("terrace", True)
        elif terrace:
            item_loader.add_value("terrace", False)

        terrace = "".join(response.xpath("//div[div[. ='Meublé']]/div/b//text()").extract()).strip()
        if terrace and terrace != "Non":
            item_loader.add_value("furnished", True)
        elif terrace:
            item_loader.add_value("furnished", False)

        swimming = "".join(response.xpath("//div[div[. ='Piscine']]/div/b//text()").extract()).strip()
        if swimming:
            if "non" in swimming.lower():
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_phone", "05.58.48.50.29")
        item_loader.add_value("landlord_name", "ERA LANDES")
        
        yield item_loader.load_item()