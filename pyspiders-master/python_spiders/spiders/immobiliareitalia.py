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
    name = 'immobiliareitalia'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliareitalia_PySpider_italy"
    start_urls = ['https://immobiliareitalia.info/offro-casa-in-affitto/?pfsearch-filter=&pfsearch-filter-order=ASC&pfsearch-filter-number=16&pfsearch-filter-ltype=16&pfsearch-filter-itype=0&pfsearch-filter-location=0&pfsearch-filter-col=grid1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//li[@class='pflist-itemtitle']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//span[@class='pf-ftitle' and contains(.,'Tipologia :')]/following-sibling::span//text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)


        external_id=response.xpath("//span[@class='pf-item-title-text']//text()").get()
        if external_id:
            external_id="".join(external_id.split("(Rif. ")[1].split(")")[0])
            item_loader.add_value("external_id",external_id)

        title=response.xpath("//span[@class='pf-item-title-text']//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u2013",""))

        rent=response.xpath("//span[@class='pfdetail-ftext pf-pricetext']//text()[1]").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1])
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//div[@class='pfdetailitem-subelement pf-onlyitem clearfix']//span[contains(.,'Superficie :')]//following-sibling::span//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("mq")[0])

        address=response.xpath("//div[@class='pf-item-title-bar']/span[2]/text()").get()
        if address:
            item_loader.add_value("address",address.split("Via")[-1].split())
        
        city = response.xpath("//span[@class='pf-ftitle'][contains(.,'Posizione')]/following-sibling::span/a/text()").get()
        if city:
            item_loader.add_value("city", city.split('-')[0].strip())
        
        room_count=response.xpath("//div[@class='pfdetailitem-subelement pf-onlyitem clearfix']//span[contains(.,'Locali :')]//following-sibling::span//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        bathroom_count=response.xpath("//div[@class='pfdetailitem-subelement pf-onlyitem clearfix']//span[contains(.,'Bagni :')]//following-sibling::span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        description="".join(response.xpath("//div[@itemprop='description']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        if description and "spese condominiali" in description.lower():
            utilities = description.split('condominiali ')[-1].split('€')[1].split(',')[0].split('.spese')[0]
            if utilities:
                item_loader.add_value("utilities",utilities)
        if description and "libero dal" in description.lower():
            desc = description.lower()
            available_date = desc.split('libero dal')[1].strip().split(' ')[0].strip().replace('/','.')
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d.%m.%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))


        images = [response.urljoin(x)for x in response.xpath("//a[contains(@rel,'prettyPhoto')]//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        latitude_longitude = response.xpath(
            "//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", " (+39) 011.32.72.342")
        item_loader.add_value("landlord_email", " imm.re.italia@gmail.com")
        item_loader.add_value("landlord_name", "Immobiliare Italia")
        terrace =response.xpath("//div[@class='pf-row']/div[contains(.,'Terrazzo')]/@class").get()
        if terrace:
            if "pfcanceldet" in terrace:
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        balcony =response.xpath("//div[@class='pf-row']/div[contains(.,'Balcone')]/@class").get()
        if balcony:
            if "pfcanceldet" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        furnished =response.xpath("//div[@class='pf-row']/div[contains(.,'Arredato')]/@class").get()
        if furnished:
            if "pfcanceldet" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        parking =response.xpath("//div[@class='pf-row']/div[contains(.,'Posto Auto')]/@class").get()
        if parking:
            if "pfcanceldet" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)




        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "camera" in p_type_string.lower():
        return "room"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None