
from re import TEMPLATE
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'breville-immobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr' 
    external_source = "BrevilleImmobilier_PySpider_france"

    def start_requests(self):
        url="https://www.breville-immobilier.com/location/1"
        yield Request(url=url,callback=self.parse,)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2) 
        seen = False
        for item in response.xpath("//article[@class='property ']//a//@href").getall():
            
            yield Request(response.urljoin(item),callback=self.populate_item,dont_filter=True,)
            seen = True
            if page == 2 or seen:
                url = f"https://www.breville-immobilier.com/location/{page}"
                yield Request(url,callback=self.parse,meta={"page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        title=response.xpath("//div[@class='title__content']/span/text()").get()
        if title:
            item_loader.add_value("title",title)
        if get_p_type_string(title): item_loader.add_value("property_type", get_p_type_string(title))
        else: return

        room_count=response.xpath("//title[.='Nombre de pièces']/parent::svg/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//title[.='Nombre de salles de bain']/parent::svg/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//title[.='Superficie en m²']/parent::svg/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0])
        rent=response.xpath("//span[contains(.,'€')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//span[@class='detail-3__reference-number']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        images=[response.urljoin(x) for x in response.xpath("//picture[@class='slider-img__picture']/parent::a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        city=response.xpath("//span[.='Ville']/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city)
        zipcode=response.xpath("//span[.='Code postal']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        item_loader.add_value("address",city+" "+zipcode)
        furnished=response.xpath("//span[.='Meublé']/following-sibling::span/text()").get()
        if furnished and "oui" in furnished.lower():
            item_loader.add_value("furnished",True)
        elevator=response.xpath("//span[.='Ascenseur']/following-sibling::span/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator",True)
        floor=response.xpath("//span[.='Etage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        description="".join(response.xpath("//div[@class='detail-3__text']//p//text() | //div[@class='detail-3__text']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        deposit=response.xpath("//span[.='Dépôt de garantie TTC']/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].replace(" ",""))
        utilities=response.xpath("//span[contains(.,'Charges locatives')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].replace(" ",""))
        item_loader.add_value("landlord_name","BREVILLE IMMOBILIER")
        item_loader.add_value("landlord_email","contact@breville-immobilier.com")
        item_loader.add_value("landlord_phone","02 31 87 88 50")

        yield item_loader.load_item()



def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "appartement" in p_type_string.lower():
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None