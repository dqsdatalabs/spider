# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import unicodedata
import dateparser
import re


class MySpider(Spider): 
    name = "alliancepartners_be"
    start_urls = ["http://www.alliance-partners.be/"]  # LEVEL 1
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source='Alliancepartners_PySpider_belgium_fr' 
    # 1. FOLLOWING
    def parse(self, response):
        
        urls = [
            {"group": "485454", "property_type": "house"},
            {"group": "485455", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for u in urls:
            start = 0
            url = f"http://www.alliance-partners.be/common/ajax/get-search-results,action=sw-item-list;type;rent:sw-item-list;pricefrom;0:sw-item-list;priceto;100000000:sw-item-list;matchinggroup;{u.get('group')}:sw-item-list;regionid;-1:sw-item-list;bought;no:sw-item-list;priceorder;ASC:sw-item-list;start;{start}:sw-item-list;count;9,rememberpage=no,state="
            yield Request(url=url,
                             callback=self.jump,
                             meta={'property_type': u.get('property_type'), 'group': u.get('group')})
                            
    def jump(self, response):
        # url ="http://www.alliance-partners.be/item/16447555/appartement-1-chambre-pour-109000-euros"
        # yield Request(url, callback=self.populate_item)
        page = response.meta.get("page", 9)

        seen = False
        for item in response.css("li.item-box > a::attr(href)").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
            
        if page == 9 or seen:
            url = f"http://www.alliance-partners.be/common/ajax/get-search-results,action=sw-item-list;type;rent:sw-item-list;pricefrom;0:sw-item-list;priceto;100000000:sw-item-list;matchinggroup;{response.meta.get('group')}:sw-item-list;regionid;-1:sw-item-list;bought;no:sw-item-list;priceorder;ASC:sw-item-list;start;{page}:sw-item-list;count;9,rememberpage=no,state="
          
            yield Request(url, callback=self.jump, meta={'property_type': response.meta.get('property_type'), 'group': response.meta.get('group'), "page": page + 9})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Alliancepartners_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//span[@itemprop='name']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title.replace("Alliance Partners",""))
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("item/")[1].split("/")[0])

        property_type = response.meta.get('property_type')
        if property_type == 'apartment':
            sub_property_type = response.xpath("//dt[contains(.,'Type de bien')]/following-sibling::dd[1]/text()").get()
            if sub_property_type:
                if 'studio' in sub_property_type.lower():
                    property_type = 'studio'
        item_loader.add_value("property_type", property_type)

        description ="".join(response.xpath("//div[@class='rte']/p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description",description.replace("Alliance Partners",""))
            if "lave vaisselles" in description:
                item_loader.add_value("dishwasher",True)
            if "parking " in description:
                item_loader.add_value("parking",True)
            if "balcon" in description:
                item_loader.add_value("balcony",True)
        dontallow=item_loader.get_output_value("description")
        if dontallow and ("loué" in dontallow.lower() or "LOUE" in dontallow):
            return 


        rent = response.xpath("//dd[@itemprop='price']/text()[contains(.,'€')]").get()
        if rent:
            rent = unicodedata.normalize("NFKD", rent)
            if "Faire offre à partir de" in rent:
                rent = rent.split("Faire offre à partir de")[1]
                item_loader.add_value("rent", rent)
            else:
                item_loader.add_value("rent", rent.split("€")[0].strip().replace(" ",""))
        
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[.='Charges locatives']/following-sibling::dd[1]/text()[contains(.,'€')]"
        ).extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        if not item_loader.get_collected_values("utilities"):
            utilities = response.xpath("//p//text()[contains(.,'Charges')]").get()
            if utilities:
                utilities = utilities.split("euro")[0].strip().split(" ")[-1].replace(",",".")
                item_loader.add_value("utilities", int(float(utilities)))
        
        bathroom_count = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[contains(.,'bains')]/following-sibling::dd[1]//text()"
        ).get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        elif not bathroom_count:
            bathroom_count = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[contains(.,'Douche')]/following-sibling::dd[1]//text()"
        ).get()
            if bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count)
        square = response.xpath(
            "//section[@class='widget item-details']/dl/dd[contains(.,'m²')]/text()"
        ).get()
        if square:
            square = square.split("m²")[0]
            item_loader.add_value("square_meters", square)
        room = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[contains(.,'Chambre')]/following-sibling::dd[1]//text()"
        ).get()
        room_studio = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[contains(.,'Type de bien')]/following-sibling::dd[1]//text()[contains(.,'Studio')]"
        ).get()
        if room:
            item_loader.add_value("room_count", room)
        if not room and room_studio:
            item_loader.add_value("room_count", "1")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//li[@class='item']/img/@data-img-big"
            ).extract()
        ]
        item_loader.add_value("images", images)
        energy_label = response.xpath(
            "//dl[@class='content data-list clearfix']/dd/img/@src"
        ).extract_first()
        if energy_label:
            item_loader.add_value(
                "energy_label", energy_label.split("_")[1].split(".")[0].upper()
            )

        available_date = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[.='Date de disponibilité']/following-sibling::dd[1]/text()"
        ).extract_first()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d %B %Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        address = response.xpath(
            "//section[@class='widget item-module item-location']/a/text()"
        ).extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))

        item_loader.add_value("landlord_phone", "065/32.11.11")
        item_loader.add_value("landlord_email", "mons@alliance-partners.be")
        item_loader.add_value("landlord_name", "Alliance Partners")

        parking = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[contains(.,'Parking')]/following-sibling::dd[1]/text()"
        ).extract_first()
        if parking:
            item_loader.add_value("parking",True)
        terrace = response.xpath(
            "//dl[@class='content data-list clearfix']/dt[contains(.,'Terrasse')]/following-sibling::dd[1]/text()"
        ).extract_first()
        if terrace:
            if "Oui" in terrace:
                item_loader.add_value("terrace",True)
        
        
        latitude_longitude = response.xpath("//script[contains(.,'Lat:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('Lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('Lng:')[2].split(',')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[0]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp.split(" ")[-2]))
        city = temp.split(" ")[-1]

        if get == "zip":
            return zip_code
        else:
            return city
