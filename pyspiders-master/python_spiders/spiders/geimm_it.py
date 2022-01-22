import scrapy
from scrapy.http.request import Request

from ..helper import extract_location_from_coordinates
from ..loaders import ListingLoader
import re


class GeimmItSpider(scrapy.Spider):
    name = 'geimm_it'
    allowed_domains = ['geimm.it']
    start_urls = ['https://www.geimm.it/it/ricerca.asp?tp=affitto']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#content-ricerca > div.tab-ric"):
            url = "https://www.geimm.it/it/" + appartment.css(
                "a.screenshot").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )
        try:
            next_page = response.css('#pagavanti').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            next_page = "https://www.geimm.it/it/" + next_page
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#content-dettaglio > div > div > table > tr > td:nth-child(1) > h2 > span::text').get()

        unwanted_titles = ['posto auto', 'garage',
                           'ufficio', 'commerciale', 'magazzino']

        for item in unwanted_titles:
            if item in title:
                return

        address = response.css(
            '#content-dettaglio > div > div > table > tr > td:nth-child(1) > h3 > strong::text').get()

        description = response.css(
            '#content-dettaglio>div>div>table>tr>td::text').extract()[2]

        feats = response.css('ul.infoimmo > li')

        external_id = None
        space = None
        rooms = None
        floor = None
        energy = None
        elevator = None
        for item in feats:
            if "Rif" in item.css('strong::text').get():
                external_id = item.css('span::text').get()
            elif "Superficie Mq:" in item.css('strong::text').get():
                try:
                    space = int(item.css('li::text').get().strip())
                except:
                    pass
            elif "Numero vani" in item.css('strong::text').get():
                try:
                    rooms = int(item.css('li::text').get().strip())
                except:
                    pass
            elif "Piano" in item.css('strong::text').get():
                try:
                    floor = item.css('li::text').get().strip()
                except:
                    pass
            elif "Classe Energetica" in item.css('strong::text').get():
                try:
                    energy = item.css('li::text').get().strip()
                except:
                    pass
            elif "Ascensore" in item.css('strong::text').get():
                if "si" in item.css('li::text').get().strip():
                    elevator = True
                else:
                    elevator = False

        rent = response.css('li.price_ric::text').get().split("€")[0].strip()
        images = response.css('a.imgzoom::attr(href)').extract()

        coords = response.css('#iframe_geomap_canvas::attr(src)').get()
        location = re.findall('-?\d+\.\d+', coords)

        try:
            zipcode, city, address = extract_location_from_coordinates(
                location[1], location[0])
        except:
            zipcode = None
            city = None
            address = None

        balcony = None
        if "balcone" in description:
            balcony = True

        furnished = None
        if "arredato" in description:
            furnished = True

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", space)
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("floor", floor)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("elevator", elevator)

        item_loader.add_value("latitude", location[0])
        item_loader.add_value("longitude", location[1])

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("energy_label", energy)

        # LandLord Details
        item_loader.add_value("landlord_name", 'Geimm servizi immobiliari')
        item_loader.add_value("landlord_phone", '06036300488')
        item_loader.add_value("landlord_email", 'info@geimm.it')

        yield item_loader.load_item()
