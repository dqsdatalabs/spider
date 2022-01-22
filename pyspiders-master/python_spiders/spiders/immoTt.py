# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser


class ImmottSpider(scrapy.Spider):
    name = "immoTt"
    allowed_domains = ["immo-tt.be", "evosys.be"]
    start_urls = (
        "https://www.immo-tt.be/index.php?page=liste&OxySeleImme=1|Appartement&OxySeleOffr=L&OxyRequete=1&OxySeleOrdre=PRIX+DESC%2CCOMM+ASC%2CCODE&submit=Afficher",
        "https://www.immo-tt.be/index.php?page=liste&OxySeleImme=0|Maison&OxySeleOffr=L&OxyRequete=1&OxySeleOrdre=PRIX+DESC%2CCOMM+ASC%2CCODE&submit=Afficher",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","
    handle_httpstatus_list = [404]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for item_responses in response.css(".zi-results .zi-item .zi-content").xpath(".//a[not(@class)]"):
            link = item_responses.xpath(".//@href").get()
            if link and not item_responses.xpath(".//div[@class='sticker']//text()"):
                yield scrapy.Request(
                    response.urljoin(link),
                    self.parse_detail,
                    cb_kwargs=dict(property_type="house" if "Maison" in response.url else "apartment"),
                )

    def parse_detail(self, response, property_type):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_xpath("title", ".//head/meta[@property='og:title']/@content")
        item_loader.add_xpath("description", ".//head/meta[@property='og:description']/@content")
        item_loader.add_xpath("rent_string", ".//div[@class='price']//text()")
        
        external_id = response.xpath(".//div[@class='reference']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        address = "".join(response.xpath("//div[@class='adresse']//text()").getall())
        if address:
            if "/" in address:
                address = address.split(":")[1].split("/")[0].strip()
                item_loader.add_value("address", address)
            elif "(" in address:
                address = address.split(":")[1].split("(")[0].strip()
                item_loader.add_value("address", address)
            else:
                address = address.split(":")[1].strip()
                item_loader.add_value("address", address)
            
            zipcode = address.strip().split(" ")[-1]
            if not zipcode.isalpha():
                item_loader.add_value("zipcode", zipcode)
        
        
        dt = response.xpath(
            ".//ul[contains(@class,'box')]//li[contains(.,'Disponibilité')]/span[@class='right']/text()"
        ).get()
        if dt:
            dt = dateparser.parse(dt)
            if dt:
                item_loader.add_value(
                    "available_date",
                    dt.date().strftime("%Y-%m-%d"),
                )
        self.get_general(response, item_loader)
        self.get_from_detail_panel(" ".join(response.xpath(".//ul[@class='box']//text()").getall()), item_loader)

        lat_lng = response.xpath(".//script[text()[contains(.,'bienLat')]]/text()").get()
        if lat_lng:
            lat = lat_lng.split("bienLat")[1].split('("')[1].split('"')[0]
            lng = lat_lng.split("bienLng")[1].split('("')[1].split('"')[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

        item_loader.add_xpath("city", "//span[@class='gros']/text()")
        square_meters = response.xpath(
            "//*[@class='box']//li[contains(.,'Superficie totale') or contains(.,'S. habitable')]/span[@class='right']//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.replace("±","").strip())
        
        item_loader.add_value("landlord_phone", "3287341122")
        item_loader.add_value("landlord_email", "info@immo-tt.be")
        item_loader.add_value("landlord_name", "Immobilier Thonnard & Trinon")
        yield scrapy.Request(
            response.xpath(".//div[@id='fullscreen-1']/img/@src").get(),
            self.parse_img,
            cb_kwargs=dict(item_loader=item_loader, num=10),
        )
        # yield item_loader.load_item()

    def parse_img(self, response, num, item_loader):
        if response.status == 404 and num <= 10:
            num -= 1
        elif response.status == 200 and num >= 10:
            num += 1
        elif (response.status == 404 and num > 10) or (response.status == 200 and num < 10):
            images = []
            add = 1
            if num < 10:
                add = 2
            for temp in range(1, num + add):
                images.append(re.sub(r"/\d{2}", f"/{temp:02d}", response.url))
            item_loader.add_value("images", images)
            yield item_loader.load_item()
            return
        yield scrapy.Request(
            re.sub(r"/\d{2}", f"/{num:02d}", response.url),
            self.parse_img,
            cb_kwargs=dict(item_loader=item_loader, num=num),
        )

    def get_general(self, response, item_loader):
        keywords = {
            "floor": "Niveau",
            "utilities": "Charges",
            "room_count": "Nbre de ch",
            "bathroom_count": "Salle de bains",
        }
        for k, v in keywords.items():
            if "count" in k:
                item_loader.add_value(
                    k,
                    response.xpath(f".//*[@class='box']//li[contains(.,'{v}')]/span[@class='right']//text()").re("\d+"),
                )
            else:
                item_loader.add_xpath(k, f".//*[@class='box']//li[contains(.,'{v}')]/span[@class='right']//text()")

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balcon",
                "nombre de balcon",
                "Nombre d",
                "balcony",
                "balcon arrière",
            ],
            "pets_allowed": ["animaux"],
            "furnished": ["meublé", "appartement meublé", "meublée"],
            "swimming_pool": ["piscine"],
            "dishwasher": ["lave-vaisselle"],
            "washing_machine": ["machine à laver", "lave linge"],
            "terrace": ["terrasse", "terrasse de repos", "terras"],
            "elevator": ["ascenseur", "ascenceur"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)
