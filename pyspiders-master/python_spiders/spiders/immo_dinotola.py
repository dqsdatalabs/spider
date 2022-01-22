import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader


class ImmoDinotolaSpider(scrapy.Spider):
    name = 'immo_dinotola'
    allowed_domains = ['immobiliaredinotola.it']
    start_urls = ['https://www.immobiliaredinotola.it/immobili/immobili_in_affitto.html']
    # start_urls = ['https://immobiliaredinotola.it/1908863/ita/affitto-appartamento-spiagge:-serapo-gaeta-latina-1908863.html']

    execution_type = 'development'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        area_urls = response.css('#listing a::attr(href)').extract()
        area_urls = ['https://immobiliaredinotola.it' + x for x in area_urls]
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        title = response.css("h1::text")[0].extract()
        if "Appartamento" in title or "Appartamentino" in title:
            property_type = "apartment"
        elif "Villa" in title:
            property_type = "house"
        else:
            return
        items = ListingItem()
        external_link = str(response.request.url)
        description = response.css(".imm-det-des::text")[0].extract()
        square_meters = response.css("#li_superficie > strong::text")[0].extract()
        room_count = response.css("#li_camere > strong::text")[0].extract()
        bathroom_count = response.css("#li_bagni > strong::text")[0].extract()
        energy_label = response.css("#li_clen::text")[0].extract()
        energy_label = energy_label[2:]
        room_count = int(room_count)
        bathroom_count = int(bathroom_count)


        try:
            rent = response.css("#sidebar > span.price.colore1.right::text")[0].extract()
            if any(char.isdigit() for char in rent):
                rent = int(''.join(x for x in rent if x.isdigit()))
            else:
                return
        except:
            pass
        currency = "EUR"
        city = response.css("#det_comune > span::text")[0].extract()
        floor = response.css("#det_piano > span::text")[0].extract()
        furnished = False
        try:
            furnish = response.css("#det_arredato > strong::text").extract()
            furnish = furnish[0]
            if "Arredato" in furnish:
                furnished = True
            else:
                furnished = False
        except:
            pass
        landlord_name = response.css(
            "#page-dettaglio > footer > div.container > div > div.span6.textright.agency > div > h3::text")[0].extract()
        landlord_phone = response.css(".agency_telephone span::text")[0].extract()

        images = response.css('.watermark img::attr(src)').extract()

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['title'] = title
        items['description'] = description
        items['property_type'] = property_type
        items['square_meters'] = square_meters
        items['room_count'] = room_count
        items['bathroom_count'] = bathroom_count
        items['floor'] = floor
        items['furnished'] = furnished
        items['energy_label'] = energy_label
        items['rent'] = rent
        items['city'] = city
        items['currency'] = currency
        items['images'] = images

        items['landlord_name'] = landlord_name
        items['landlord_phone'] = landlord_phone

        yield items
